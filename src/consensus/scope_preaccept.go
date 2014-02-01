package consensus

import (
	"time"
)

import (
	"node"
)

func makePreAcceptCommitTimeout() time.Time {
	return time.Now().Add(time.Duration(PREACCEPT_COMMIT_TIMEOUT) * time.Millisecond)
}

func (s *Scope) preAcceptInstanceUnsafe(inst *Instance, incrementBallot bool) error {
	var instance *Instance
	if existing, exists := s.instances[inst.InstanceID]; exists {
		if existing.Status > INSTANCE_PREACCEPTED {
			return NewInvalidStatusUpdateError(existing, INSTANCE_PREACCEPTED)
		}
		instance = existing
		instance.Noop = inst.Noop
	} else {
		instance = inst
	}

	instance.Status = INSTANCE_PREACCEPTED
	instance.Dependencies = s.getCurrentDepsUnsafe()
	instance.Sequence = s.getNextSeqUnsafe()
	instance.commitTimeout = makePreAcceptCommitTimeout()
	if incrementBallot {
		instance.MaxBallot++
	}
	s.inProgress.Add(instance)
	s.instances.Add(instance)

	if err := s.Persist(); err != nil {
		return err
	}

	return nil
}

// sets the given instance to preaccepted and updates, deps,
// seq, and commit timeout
// in the case of handling messages from leaders to replicas
// the message instance should be passed in. It will either
// update the existing instance in place, or add the message
// instance to the scope's instance
// returns a bool indicating that the instance was actually
// accepted (and not skipped), and an error, if applicable
func (s *Scope) preAcceptInstance(instance *Instance, incrementBallot bool) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.preAcceptInstanceUnsafe(instance, incrementBallot)
}

// sends pre accept responses to the given replicas, and returns their responses. An error will be returned
// if there are problems, or a quorum of responses were not received within the timeout
func (s *Scope) sendPreAccept(instance *Instance, replicas []node.Node) ([]*PreAcceptResponse, error) {
	recvChan := make(chan *PreAcceptResponse, len(replicas))

	instanceCopy, err := s.copyInstanceAtomic(instance)
	if err != nil {
		return nil, err
	}
	msg := &PreAcceptRequest{Scope: s.name, Instance: instanceCopy}

	sendMsg := func(n node.Node) {
		logger.Debug("Preaccept: Sending message to node %v for instance %v", n.GetId(), instance.InstanceID)
		if response, err := n.SendMessage(msg); err != nil {
			logger.Warning("Error receiving PreAcceptResponse: %v", err)
		} else {
			if preAccept, ok := response.(*PreAcceptResponse); ok {
				logger.Debug("Preaccept: response received from node %v for instance %v", n.GetId(), instance.InstanceID)
				recvChan <- preAccept
			} else {
				logger.Warning("Unexpected PreAccept response type: %T", response)
			}
		}
	}

	// lock the scope so other goroutines
	// don't changes attributes on the instance
	for _, replica := range replicas {
		go sendMsg(replica)
	}

	numReceived := 1  // this node counts as a response
	quorumSize := ((len(replicas) + 1) / 2) + 1
	timeoutEvent := getTimeoutEvent(time.Duration(PREACCEPT_TIMEOUT) * time.Millisecond)
	var response *PreAcceptResponse
	responses := make([]*PreAcceptResponse, 0, len(replicas))
	for numReceived < quorumSize {
		select {
		case response = <-recvChan:
			logger.Debug("PreAccept response received: %v", instance.InstanceID)
			responses = append(responses, response)
			numReceived++
		case <-timeoutEvent:
			logger.Debug("PreAccept timeout for instance: %v", instance.InstanceID)
			return nil, NewTimeoutError("Timeout while awaiting pre accept responses")
		}
	}

	// check if any of the messages were rejected
	accepted := true
	for _, response := range responses {
		accepted = accepted && response.Accepted
	}

	// handle rejected pre-accept messages
	if !accepted {
		logger.Debug("PreAccept request rejected for instance %v", instance.InstanceID)
		// update max ballot from responses
		bmResponses := make([]BallotMessage, len(responses))
		for i, response := range responses {
			bmResponses[i] = BallotMessage(response)
		}
		s.updateInstanceBallotFromResponses(instance, bmResponses)
		return nil, NewBallotError("Ballot number rejected")
	}

	return responses, nil
}

// merges the attributes from the pre accept responses onto the local instance
// and returns a bool indicating if any changes were made
func (s *Scope) mergePreAcceptAttributes(instance *Instance, responses []*PreAcceptResponse) (bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	logger.Debug("Merging preaccept attributes")
	changes := false
	for _, response := range responses {
		changes = changes || instance.mergeAttributes(response.Instance.Sequence, response.Instance.Dependencies)
		if len(response.MissingInstances) > 0 {
			logger.Debug("Merging preaccept: adding %v missing instances", len(response.MissingInstances))
			if err := s.addMissingInstancesUnsafe(response.MissingInstances...); err != nil {
				return changes, err
			}
		}
	}
	if err := s.Persist(); err != nil {
		return true, err
	}
	logger.Debug("Preaccept attributes merged")
	return changes, nil
}

// assigned to var for testing
var scopePreAcceptPhase = func(s *Scope, instance *Instance) (acceptRequired bool, err error) {
	replicas := s.manager.getScopeReplicas(s)

	if err := s.preAcceptInstance(instance, true); err != nil {
		// this may be possible during an explicit prepare
		if _, ok := err.(InvalidStatusUpdateError); !ok {
			return false, err
		}
	}

	// send instance pre-accept to replicas
	paResponses, err := s.sendPreAccept(instance, replicas)
	if err != nil {
		// quorum failed, a later explicit prepare may
		// fix it but nothing can be done now
		return false, err
	}

	return s.mergePreAcceptAttributes(instance, paResponses)
}

// runs the full preaccept phase for the given instance, returning
// a bool indicating if an accept phase is required or not
func (s *Scope) preAcceptPhase(instance *Instance) (acceptRequired bool, err error) {
	logger.Debug("PreAccept phase started")
	acceptRequired, err = scopePreAcceptPhase(s, instance)
	logger.Debug("Preaccept phase completed: %v %v", acceptRequired, err)
	return acceptRequired, err
}


// handles a preaccept message from the command leader for an instance
// this executes the replica preaccept phase for the given instance
func (s *Scope) HandlePreAccept(request *PreAcceptRequest) (*PreAcceptResponse, error) {
	logger.Debug("PreAccept message for %v received, ballot: %v", request.Instance.InstanceID, request.Instance.MaxBallot)
	s.lock.Lock()
	defer s.lock.Unlock()

	extSeq := request.Instance.Sequence
	extDeps := NewInstanceIDSet(request.Instance.Dependencies)

	instance := request.Instance
	if err := s.preAcceptInstanceUnsafe(instance, false); err != nil {
		if _, ok := err.(InvalidStatusUpdateError); !ok {
			return nil, err
		}
	}

	// check agreement on seq and deps with leader
	instance = s.instances[request.Instance.InstanceID]
	newDeps := NewInstanceIDSet(instance.Dependencies)
	instance.DependencyMatch = extSeq == instance.Sequence && extDeps.Equal(newDeps)

	if err := s.Persist(); err != nil {
		return nil, err
	}

	missingDeps := newDeps.Subtract(extDeps)
	reply := &PreAcceptResponse{
		Accepted:         true,
		MissingInstances: make([]*Instance, 0, len(missingDeps)),
	}

	if instanceCopy, err := s.copyInstanceUnsafe(instance); err != nil {
		return nil, err
	} else {
		reply.Instance = instanceCopy
		reply.MaxBallot = instanceCopy.MaxBallot
	}

	for iid := range missingDeps {
		inst := s.instances[iid]
		if inst != nil {
			if instanceCopy, err := s.copyInstanceUnsafe(inst); err != nil {
				return nil, err
			} else {
				reply.MissingInstances = append(reply.MissingInstances, instanceCopy)
			}
		}
	}

	logger.Debug("PreAccept message for %v replied with accepted: %v", request.Instance.InstanceID, reply.Accepted)
	if len(reply.MissingInstances) > 0 {
		logger.Debug("PreAccept reply for %v includes %v missing instances", request.Instance.InstanceID, len(reply.MissingInstances))
	}
	return reply, nil
}

