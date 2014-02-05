package consensus

import (
	"fmt"
	"sync"
	"time"
)

import (
	"node"
)

// sends and explicit prepare request to the other nodes. Returns a channel for receiving responses on
// This method does not wait for the responses to return, because the scope needs to be locked during
// message sending, so the instance is not updated, but does not need to be locked while receiving responses
func (s *Scope) sendPrepare(instance *Instance, replicas []node.Node) (<- chan *PrepareResponse, error) {
	recvChan := make(chan *PrepareResponse, len(replicas))
	msg := &PrepareRequest{Scope:s.name, Ballot:instance.MaxBallot, InstanceID:instance.InstanceID}
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

	return recvChan, nil
}

func (s *Scope) receivePrepareResponseQuorum(recvChan <-chan *PrepareResponse, instance *Instance, quorumSize int, numReplicas int) ([]*PrepareResponse, error) {
	numReceived := 1  // this node counts as a response
	timeoutEvent := getTimeoutEvent(time.Duration(PREPARE_TIMEOUT) * time.Millisecond)
	var response *PrepareResponse
	responses := make([]*PrepareResponse, 0, numReplicas)
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

	accepted := true
	var maxBallot uint32
	for _, response := range responses {
		if response.Instance != nil {
			// TODO: update local status if the response status is higher than the local status
			if ballot := response.Instance.MaxBallot; ballot > maxBallot {
				maxBallot = ballot
			}
		}
		accepted = accepted && response.Accepted
	}
	if !accepted {
		// update ballot
		updateBallot := func() error {
			s.lock.Lock()
			defer s.lock.Unlock()
			if maxBallot > instance.MaxBallot {
				instance.MaxBallot = maxBallot
				if err := s.Persist(); err != nil {
					return err
				}
			}
			return nil
		}
		if err := updateBallot(); err != nil {
			return nil, err
		}
		logger.Debug("Prepare request(s) rejected")
		return nil, NewBallotError("Prepare request(s) rejected")
	}

	return responses, nil
}

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

	referenceInstance, err := s.copyInstanceUnsafe(localInstance)
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

	// increments and sends the prepare messages in a single lock
	// TODO: combine send and receive methods since the instance is being copied now
	incrementAndCopyInstance := func() (*Instance, error) {
		s.lock.Lock()
		defer s.lock.Unlock()
		instance.MaxBallot++
		if err := s.Persist(); err != nil {
			return nil, err
		}
		return s.copyInstanceUnsafe(instance)
	}
	instanceCopy, err := incrementAndCopyInstance()
	if err != nil { return nil, err }
	recvChan, err := s.sendPrepare(instanceCopy, replicas)
	if err != nil { return nil, err }

	// receive responses from at least a quorum of nodes
	quorumSize := ((len(replicas) + 1) / 2) + 1
	return s.receivePrepareResponseQuorum(recvChan, instance, quorumSize, len(replicas))
}

// sends prepare messages to the replicas and returns an instance used
// to determine how to proceed. This will succeed even if the local instance
// is using an out of date ballot number. The prepare caller will have to work
// out what to do (fail or retry)
// assigned to a var to aid in testing
var scopePreparePhase1 = func(s *Scope, instance *Instance) (*Instance, error) {
	responses, err := scopeSendPrepare(s, instance)
	if err != nil { return nil, err }
	return s.analyzePrepareResponses(responses), nil
}

// uses the remote instance to start a preaccept phase, an accept phase, or a commit phase
var scopePreparePhase2 = func(s *Scope, instance *Instance, remoteInstance *Instance) error {
	// checks the remote instance ballot against
	// the local instance, and increases the local
	// instance ballot if the remote is larget
	checkAndMatchBallot := func() error {
		s.lock.Lock()
		defer s.lock.Unlock()
		if remoteInstance.MaxBallot > instance.MaxBallot {
			instance.MaxBallot = remoteInstance.MaxBallot
			if err := s.Persist(); err != nil {
				return err
			}
		}
		return nil
	}

	// sets the instance's noop flag to true
	setNoop := func() error {
		s.lock.Lock()
		defer s.lock.Unlock()
		instance.Noop = true
		if err := s.Persist(); err != nil {
			return err
		}
		return nil
	}
//
//	var status InstanceStatus
//	var prepareInstance *Instance
//	if remoteInstance != nil {
//		if err := checkAndMatchBallot(); err != nil {
//			return nil
//		}
//		prepareInstance = remoteInstance
//		status = remoteInstance.Status
//	} else {
//		logger.Warning("Instance %v not recognized by other replicas, committing noop", instance.InstanceID)
//		setNoop()
//		status = INSTANCE_PREACCEPTED
//		prepareInstance = instance
//	}

	var status InstanceStatus
	var prepareInstance *Instance
	if remoteInstance != nil {
		if instance.Status > remoteInstance.Status {
			prepareInstance = instance
		} else {
			if err := checkAndMatchBallot(); err != nil {
				return nil
			}
			prepareInstance = remoteInstance
		}
		status = prepareInstance.Status
	} else {
		if instance.Status <= INSTANCE_PREACCEPTED {
			logger.Warning("Instance %v not recognized by other replicas, committing noop", instance.InstanceID)
			setNoop()
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
	getPrepareMutex := func() *sync.Mutex {
		s.lock.Lock()
		defer s.lock.Unlock()
		lock := s.prepareLock[instance.InstanceID]
		if lock == nil {
			lock = &sync.Mutex{}
			s.prepareLock[instance.InstanceID] = lock
		}
		return lock
	}
	pLock := getPrepareMutex()
	pLock.Lock()
	defer pLock.Unlock()

	// check if the instance has been committed
	isCommitted := func() bool {
		s.lock.Lock()
		defer s.lock.Unlock()
		return instance.Status >= INSTANCE_COMMITTED
	}
	if isCommitted() {
		return nil
	}

	// TODO: add prepare notify?
	remoteInstance, err := scopePreparePhase1(s, instance)
	if err != nil { return err }

	err = scopePreparePhase2(s, instance, remoteInstance)
	if err != nil {
		return err
	}

	delete(s.prepareLock, instance.InstanceID)

	return nil
}

// runs explicit prepare phase on instances where a command leader failure is suspected
// during execution,
func (s *Scope) preparePhase(instance *Instance) error {
	s.lock.Lock()
	logger.Debug("Prepare phase started for %v with status %v", instance.InstanceID, instance.Status)
	status := instance.Status
	if status >= INSTANCE_COMMITTED {
		s.lock.Unlock()
		return nil
	}
	if time.Now().After(instance.commitTimeout) {
		logger.Debug("Prepare: commit grace period expired. proceeding")
		// proceed, the instance's commit grace period
		// has expired
		s.statCommitTimeout++
		s.lock.Unlock()
	} else {
		logger.Debug("Prepare: waiting on commit grace period to expire")
		// get or create broadcast object
		cond, ok := s.commitNotify[instance.InstanceID]
		if !ok {
			cond = makeConditional()
			s.commitNotify[instance.InstanceID] = cond
		}

		// wait on broadcast event or timeout
		broadcastEvent := make(chan bool)
		go func() {
			cond.L.Lock()
			cond.Wait()
			cond.L.Unlock()
			broadcastEvent <- true
		}()
		timeoutEvent := getTimeoutEvent(instance.commitTimeout.Sub(time.Now()))
		s.lock.Unlock()
		select {
		case <- broadcastEvent:
			logger.Debug("Prepare: commit broadcast event received for %v", instance.InstanceID)
			// instance was executed by another goroutine
			s.statCommitTimeoutWait++
			return nil
		case <- timeoutEvent:
			logger.Debug("Prepare: commit grace period expired for %v. proceeding", instance.InstanceID)
			// execution timed out
			s.lock.Lock()

			// check that instance was not executed by another
			// waking goroutine
			if instance.Status >= INSTANCE_COMMITTED {
				// unlock and continue if it was
				s.lock.Unlock()
				return nil
			} else {
				s.statCommitTimeout++
				s.statCommitTimeoutWait++
				s.lock.Unlock()
			}
		}
	}

	logger.Debug("Prepare phase started")
	err := scopePreparePhase(s, instance)
	logger.Debug("Prepare phase completed")
	return err
}

// handles a prepare message from an instance attempting to take
// control of an instance.
func (s *Scope) HandlePrepare(request *PrepareRequest) (*PrepareResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	instance := s.instances[request.InstanceID]
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
		}
		if instanceCopy, err := s.copyInstanceUnsafe(instance); err != nil {
			return nil, err
		} else {
			response.Instance = instanceCopy
		}
		responseBallot = response.Instance.MaxBallot
	}

	logger.Debug("Prepare message for %v replied with accept: %v (%v)", request.InstanceID, response.Accepted, responseBallot)
	return response, nil
}
