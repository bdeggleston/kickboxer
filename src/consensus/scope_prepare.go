package consensus

import (
	"fmt"
	"time"
)

import (
	"node"
)

// analyzes the responses to a prepare request, and returns an instance
// the prepare phase should use to determine how to proceed
func (s *Scope) analyzePrepareResponses(responses []*PrepareResponse) (*Instance) {
	otherInstancesSeen := false

	// find the highest response ballot
	maxBallot := uint32(0)
	for _, response := range responses {
		if response.Instance != nil {
			otherInstancesSeen = true
			if ballot := response.Instance.MaxBallot; ballot > maxBallot {
				maxBallot = ballot
			}
		}
	}

	if !otherInstancesSeen {
		return nil
	}

	// find the highest response status
	maxStatus := InstanceStatus(byte(0))
	var instance *Instance
	for _, response := range responses {
		if response.Instance == nil {
			continue
		}
		if status := response.Instance.Status; status > maxStatus {
			maxStatus = status
			if response.Instance != nil {
				instance = response.Instance
			}
		}
	}

	return instance
}

// TODO: not used/done yet, replaces analyzePrepareResponses
// transforms the local instance from the prepare responses
func (s *Scope) applyPrepareResponses(responses []*PrepareResponse, localInstance *Instance) (*Instance, error) {
	var maxBallot uint32
	var maxStatus InstanceStatus

	// indicates that nil instances
	// were encountered in the responses
	var nilInstances bool

	// indicates that non nil instances
	// were encountered in the responses
	var otherInstances bool

	// get info about the response instances
	for _, response := range responses {
		if response.Instance != nil {
			otherInstances = true
			if ballot := response.Instance.MaxBallot; ballot > maxBallot {
				maxBallot = ballot
			}
			if status := response.Instance.Status; status > maxStatus {
				maxStatus = status
			}
		} else {
			nilInstances = true
		}
	}

	referenceInstance, err := localInstance.Copy()
	if err != nil {
		return nil, err
	}

	// if no other instances have been seen...
	if !otherInstances {
		if referenceInstance.Status != INSTANCE_PREACCEPTED {
			return nil, fmt.Errorf(
				"Instance %v is unknown by other nodes, but has a status of %v",
				referenceInstance.InstanceID,
				referenceInstance.Status,
			)
		}
		referenceInstance.Noop = true
		return referenceInstance, nil
	}

	// TODO: do all of the ballots need to match to advance the preaccept process? Yes, and the prepare messages need to be accepted
	if maxStatus == INSTANCE_PREACCEPTED && !nilInstances {

	} else if maxStatus == INSTANCE_PREACCEPTED {

	}
	return nil, nil
}

var scopeSendPrepare = func(s *Scope, instance *Instance) ([]*PrepareResponse, error) {
	replicas := s.manager.getScopeReplicas(s)

	// increment ballot and return message object
	getMsg := func() (*PrepareRequest, error) {
		s.lock.Lock()
		defer s.lock.Unlock()
		instance.MaxBallot++
		msg := &PrepareRequest{Scope:s.name, Ballot:instance.MaxBallot, InstanceID:instance.InstanceID}
		if err := s.Persist(); err != nil {
			return nil, err
		}
		return msg, nil
	}
	msg, err := getMsg()
	if err != nil {
		return nil, err
	}

	recvChan := make(chan *PrepareResponse, len(replicas))
	sendMsg := func(n node.Node) {
		logger.Debug("Sending prepare request to node %v", n.GetId())
		if response, err := n.SendMessage(msg); err != nil {
			logger.Warning("Error receiving PrepareResponse: %v", err)
		} else {
			if preAccept, ok := response.(*PrepareResponse); ok {
				recvChan <- preAccept
			} else {
				logger.Warning("Unexpected Prepare response type: %T", response)
			}
		}
	}

	for _, replica := range replicas {
		go sendMsg(replica)
	}

	// receive responses from at least a quorum of nodes
	quorumSize := ((len(replicas) + 1) / 2) + 1
	numReceived := 1  // this node counts as a response
	timeoutEvent := getTimeoutEvent(time.Duration(PREPARE_TIMEOUT) * time.Millisecond)
	var response *PrepareResponse
	responses := make([]*PrepareResponse, 0, len(replicas))
	for numReceived < quorumSize {
		select {
		case response = <-recvChan:
			logger.Debug("Prepare response received: %v", instance.InstanceID)
			responses = append(responses, response)
			numReceived++
		case <-timeoutEvent:
			logger.Debug("Prepare timeout for instance: %v", instance.InstanceID)
			return nil, NewTimeoutError("Timeout while awaiting pre accept responses")
		}
	}

	// finally, receive any additional responses
	drain: for {
		select {
		case response = <-recvChan:
			responses = append(responses, response)
			numReceived++
		default:
			break drain
		}
	}

	return responses, nil
}

// performs an initial check of the responses
// if any of the responses have been rejected, the local ballot will be updated,
// as well as it's status if there are any responses with a higher status
var scopePrepareCheckResponses = func(s *Scope, instance *Instance, responses []*PrepareResponse) error {
	accepted := true
	var maxBallot uint32
	var maxStatus InstanceStatus
	for _, response := range responses {
		if response.Instance != nil {
			// TODO: update local status if the response status is higher than the local status
			if ballot := response.Instance.MaxBallot; ballot > maxBallot {
				maxBallot = ballot
			}
			if status := response.Instance.Status; status > maxStatus {
				maxStatus = status
			}
		}
		accepted = accepted && response.Accepted
	}
	if !accepted {
		var maxInstance *Instance
		for _, response := range responses {
			if response.Instance != nil {
				rInstance := response.Instance
				if rInstance.MaxBallot == maxBallot && rInstance.Status == maxStatus {
					maxInstance = rInstance
				}
			}
		}

		// update local ballot and/or status
		if maxInstance != nil && maxInstance.Status > instance.Status {
			switch maxInstance.Status {
			case INSTANCE_ACCEPTED:
				if err := s.acceptInstance(maxInstance, false); err != nil {
					return err
				}
			case INSTANCE_COMMITTED, INSTANCE_EXECUTED:
				if err := s.commitInstance(maxInstance, false); err != nil {
					return err
				}
			}
		}

		logger.Debug("Prepare request(s) rejected")
		return NewBallotError("Prepare request(s) rejected")
	}

	return nil
}

// uses the remote instance to start a preaccept phase, an accept phase, or a commit phase
var scopePrepareApply = func(s *Scope, instance *Instance, responses []*PrepareResponse) error {
	if err := scopePrepareCheckResponses(s, instance, responses); err != nil {
		return err
	}

	remoteInstance := s.analyzePrepareResponses(responses)
	var status InstanceStatus
	var prepareInstance *Instance
	if remoteInstance != nil {
		if instance.Status > remoteInstance.Status {
			prepareInstance = instance
		} else {
			prepareInstance = remoteInstance
		}
		status = prepareInstance.Status
	} else {
		if instance.Status <= INSTANCE_PREACCEPTED {
			logger.Warning("Instance %v not recognized by other replicas, committing noop", instance.InstanceID)
			instance.setNoop()
			status = INSTANCE_PREACCEPTED
			prepareInstance = instance
		} else {
			prepareInstance = instance
			status = INSTANCE_PREACCEPTED
		}
	}


	// for the first step that prepare takes (preaccept, accept, commit), it should use
	// the remote instance, since the remote instance may have newer deps and seq if it's
	// been accepted/committed. For all steps afterwards though, the local instance should
	// be used, since the remote instance will not be updated after each step
	// TODO: send back instances when a message/update is rejected, so the calling node can update itself

	// TODO: consider running the full prepare phase only for preaccepted responses, otherwise, just accept/commit locally
	acceptRequired := true
	var err error
	switch status {
	case INSTANCE_PREACCEPTED:
		// run pre accept phase
		logger.Debug("Prepare phase starting at PreAccept phase for %v on %v", instance.InstanceID, s.GetLocalID())
		acceptRequired, err = s.preAcceptPhase(prepareInstance)
		if err != nil {
			logger.Debug("Prepare PreAccept error for %v on %v: %v", instance.InstanceID, s.GetLocalID(), err)
			return err
		}
		prepareInstance = instance
		fallthrough
	case INSTANCE_ACCEPTED:
		// run accept phase
		if acceptRequired {
			logger.Debug("Prepare phase starting at Accept phase for %v on %v", instance.InstanceID, s.GetLocalID())
			// use the remote instance to initiate the new accept phase, otherwise
			// the existing (potentially incorrect) attributes will be used for the accept
			err = s.acceptPhase(prepareInstance)
			if err != nil {
				logger.Debug("Prepare Accept error for %v on %v: %v", instance.InstanceID, s.GetLocalID(), err)
				return err
			}
			prepareInstance = instance
		}
		fallthrough
	case INSTANCE_COMMITTED, INSTANCE_EXECUTED:
		// commit instance
		logger.Debug("Prepare phase starting at Commit phase for %v on %v", instance.InstanceID, s.GetLocalID())
		// use the remote instance to initiate the new commit phase, otherwise
		// the existing (potentially incorrect) attributes will be committed
		err = s.commitPhase(prepareInstance)
		if err != nil {
			logger.Debug("Prepare Commit error for %v on %v: %v", instance.InstanceID, s.GetLocalID(), err)
			return err
		}
	default:
		return fmt.Errorf("Unknown instance status: %v", remoteInstance.Status)
	}

	return nil
}

var scopePreparePhase = func(s *Scope, instance *Instance) error {
	instance.prepareLock.Lock()
	defer instance.prepareLock.Unlock()

	// check if the instance has been committed
	isCommitted := func() bool {
		s.lock.Lock()
		defer s.lock.Unlock()
		return instance.Status >= INSTANCE_COMMITTED
	}
	if isCommitted() {
		return nil
	}

	responses, err := scopeSendPrepare(s, instance)
	if err != nil { return err }

	err = scopePrepareApply(s, instance, responses)
	if err != nil {
		return err
	}

	delete(s.prepareLock, instance.InstanceID)

	return nil
}

// TODO: test alone
// attempts to defer the prepare phase to successor nodes
// returns a bool indicating if the prepare should proceed locally
var scopeDeferToSuccessor = func(s *Scope, instance *Instance) (bool, error) {

	// make a map of replicas
	replicas := s.manager.getScopeReplicas(s)
	replicaMap := make(map[node.NodeId]node.Node, len(replicas))
	for _, replica := range replicas {
		replicaMap[replica.GetId()] = replica
	}

	for _, nid := range instance.Successors {
		if nid == s.GetLocalID() {
			break
		}
		if replica, exists := replicaMap[nid]; exists {
			recvChan := make(chan *PrepareSuccessorResponse)
			errChan := make(chan bool)
			go func() {
				msg := &PrepareSuccessorRequest{InstanceID: instance.InstanceID, Scope: s.name}
				logger.Debug("Prepare Successor: Sending message to node %v for instance %v", replica.GetId(), instance.InstanceID)
				if response, err := replica.SendMessage(msg); err != nil {
					logger.Warning("Error receiving PrepareSuccessorResponse: %v", err)
					errChan <- true
				} else {
					if preAccept, ok := response.(*PrepareSuccessorResponse); ok {
						logger.Debug("Prepare Successor: response received from node %v for instance %v", replica.GetId(), instance.InstanceID)
						recvChan <- preAccept
					} else {
						logger.Warning("Unexpected Prepare Successor response type: %T", response)
					}
				}
			}()
			var response *PrepareSuccessorResponse
			select {
			case response = <- recvChan:
				logger.Debug("Prepare Successor: response received from %v for instance %v", nid, instance.InstanceID)
				if response.Instance == nil {
					// successor is not aware of instance, go to the next successor
					logger.Debug("Prepare Successor: nil instance received from %v for instance %v", nid, instance.InstanceID)
					continue
				}

				updateInstance := func() (bool, error) {
					var proceed bool
					var err error
					s.lock.Lock()
					defer s.lock.Unlock()
					if response.Instance.Status > instance.Status {
						switch response.Instance.Status {
						case INSTANCE_ACCEPTED:
							err = s.acceptInstanceUnsafe(instance, false)
						case INSTANCE_COMMITTED, INSTANCE_EXECUTED:
							proceed = true
							err = s.commitInstanceUnsafe(instance, false)
						}
					}
					if err != nil {
						return false, err
					}
					return proceed, nil
				}
				return updateInstance()

			case <- errChan:
				// there was an error communicating with the successor, go to the next one
				// the send/receive go routine handles the logging

			case <- instance.getCommitEvent().getChan():
				// instance was committed while waiting for a PrepareSuccessorResponse
				logger.Debug("Prepare Successor: commit event received for instance %v", instance.InstanceID)
				return true, nil

			case <- getTimeoutEvent(time.Duration(SUCCESSOR_TIMEOUT) * time.Millisecond):
				// timeout, go to the next successor
				logger.Debug("Prepare Successor: timeout communicating with %v for instance %v", nid, instance.InstanceID)

			}
		}

	}
	return true, nil
}

// TODO: test alone
// determines a prepare phase should proceed for the given instance
// will block if waiting on a timeout / notification
func (s *Scope) prepareShouldProceed(instance *Instance) bool {
	// gets the instance status and timeout in a lock
	getStatusAndTimeout := func() (InstanceStatus, time.Time) {
		s.lock.RLock()
		defer s.lock.RUnlock()
		return instance.Status, instance.commitTimeout
	}

	status, timeout := getStatusAndTimeout()

	// bail out if the instance has already been committed
	if status >= INSTANCE_COMMITTED {
		return false
	}

	if time.Now().After(timeout) {
		logger.Debug("Prepare: commit grace period expired. proceeding")
		// proceed, the instance's commit grace period
		// has expired
		s.statCommitTimeout++
	} else {
		logger.Debug("Prepare: waiting on commit grace period to expire")
		select {
		case <- instance.getCommitEvent().getChan():
			logger.Debug("Prepare: commit broadcast event received for %v", instance.InstanceID)
			// instance was executed by another goroutine
			s.statCommitTimeoutWait++
			return false
		case <- getTimeoutEvent(timeout.Sub(time.Now())):
			logger.Debug("Prepare: commit grace period expired for %v. proceeding", instance.InstanceID)
			// execution timed out

			// check that instance was not executed by another
			// waking goroutine
			status, _ = getStatusAndTimeout()
			if status >= INSTANCE_COMMITTED {
				// unlock and continue if it was
				return false
			} else {
				s.statCommitTimeout++
				s.statCommitTimeoutWait++
			}
		}
	}
	return true
}

var scopePrepareInstance = func(s *Scope, instance *Instance) error {
	if !s.prepareShouldProceed(instance) {
		return nil
	}

	//
	for proceed, err := scopeDeferToSuccessor(s, instance); !proceed || err != nil; {
		if err != nil {
			return err
		}
		select {
		case <- getTimeoutEvent(time.Duration(SUCCESSOR_CONTACT_INTERVAL)):
			// interval passed, contact successor again
		case <- instance.getCommitEvent().getChan():
			// instance was committed
			return nil
		}
	}

	// double check we should proceed
	if !s.prepareShouldProceed(instance) {
		return nil
	}

	logger.Debug("Prepare phase started")
	err := scopePreparePhase(s, instance)
	logger.Debug("Prepare phase completed")
	return err
}

// TODO: rename to prepareInstance
// runs explicit prepare phase on instances where a command leader failure is suspected
// during execution,
func (s *Scope) preparePhase(instance *Instance) error {
	return scopePrepareInstance(s, instance)
}

// handles a prepare message from an instance attempting to take
// control of an instance.
func (s *Scope) HandlePrepare(request *PrepareRequest) (*PrepareResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	instance := s.instances.Get(request.InstanceID)
	logger.Debug("Prepare message received for instance %v, ballot: %v", request.InstanceID, request.Ballot)
	response := &PrepareResponse{}
	var responseBallot uint32
	if instance == nil {
		response.Accepted = true
	} else {
		response.Accepted = request.Ballot > instance.MaxBallot
		if response.Accepted {
			instance.MaxBallot = request.Ballot
			if err := s.Persist(); err != nil {
				return nil, err
			}
		} else {
			logger.Debug("Prepare message rejected for %v, %v >= %v", request.InstanceID, instance.MaxBallot, request.Ballot)
		}
		if instanceCopy, err := instance.Copy(); err != nil {
			return nil, err
		} else {
			response.Instance = instanceCopy
		}
		responseBallot = response.Instance.MaxBallot
	}

	logger.Debug("Prepare message for %v replied with accept: %v (%v)", request.InstanceID, response.Accepted, responseBallot)
	return response, nil
}

// handles a message from a replica requesting that a prepare phase is executed on the given instance
func (s *Scope) HandlePrepareSuccessor(request *PrepareSuccessorRequest) (*PrepareSuccessorResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	response := &PrepareSuccessorResponse{}
	if instance := s.instances.Get(request.InstanceID); instance != nil {
		response.Instance = instance
		if instance.Status < INSTANCE_COMMITTED {
			s.preparePhase(instance)
		}
	}
	return response, nil
}



