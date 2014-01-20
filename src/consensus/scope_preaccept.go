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

func (s *Scope) preAcceptInstanceUnsafe(instance *Instance) (bool, error) {
	if existing, exists := s.instances[instance.InstanceID]; exists {
		if existing.Status >= INSTANCE_PREACCEPTED {
			return false, nil
		}
	} else {
		s.instances.Add(instance)
	}

	instance.Status = INSTANCE_PREACCEPTED
	instance.Dependencies = s.getCurrentDepsUnsafe()
	instance.Sequence = s.getNextSeqUnsafe()
	instance.commitTimeout = makePreAcceptCommitTimeout()
	s.inProgress.Add(instance)

	if err := s.Persist(); err != nil {
		return false, err
	}

	return true, nil
}

// sets the given instance to preaccepted and updates, deps,
// seq, and commit timeout
// in the case of handling messages from leaders to replicas
// the message instance should be passed in. It will either
// update the existing instance in place, or add the message
// instance to the scope's instance
// returns a bool indicating that the instance was actually
// accepted (and not skipped), and an error, if applicable
func (s *Scope) preAcceptInstance(instance *Instance) (bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.preAcceptInstanceUnsafe(instance)
}

// sends pre accept responses to the given replicas, and returns their responses. An error will be returned
// if there are problems, or a quorum of responses were not received within the timeout
func (s *Scope) sendPreAccept(instance *Instance, replicas []node.Node) ([]*PreAcceptResponse, error) {
	// TODO: preaccept sending needs to happen in a lock
	recvChan := make(chan *PreAcceptResponse, len(replicas))
	msg := &PreAcceptRequest{Scope: s.name, Instance: instance}
	sendMsg := func(n node.Node) {
		if response, err := n.SendMessage(msg); err != nil {
			logger.Warning("Error receiving PreAcceptResponse: %v", err)
		} else {
			if preAccept, ok := response.(*PreAcceptResponse); ok {
				recvChan <- preAccept
			} else {
				logger.Warning("Unexpected PreAccept response type: %T", response)
			}
		}
	}
	for _, replica := range replicas {
		go sendMsg(replica)
	}

	numReceived := 1  // this node counts as a response
	quorumSize := ((len(replicas) + 1) / 2) + 1
	timeoutEvent := time.After(time.Duration(PREACCEPT_TIMEOUT) * time.Millisecond)
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
	changes := false
	for _, response := range responses {
		changes = changes || instance.mergeAttributes(response.Instance.Sequence, response.Instance.Dependencies)
	}
	if err := s.Persist(); err != nil {
		return true, err
	}
	// TODO: handle MissingInstances included in the responses
	return changes, nil
}

// assigned to var for testing
var scopePreAcceptPhase = func(s *Scope, instance *Instance) (acceptRequired bool, err error) {
	replicas := s.manager.getScopeReplicas(s)

	if success, err := s.preAcceptInstance(instance); err != nil {
		return false, err
	} else if !success {
		// how would this even happen?
		panic("instance already exists")
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
	return scopePreAcceptPhase(s, instance)
}


// handles a preaccept message from the command leader for an instance
// this executes the replica preaccept phase for the given instance
func (s *Scope) HandlePreAccept(request *PreAcceptRequest) (*PreAcceptResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	extSeq := request.Instance.Sequence
	extDeps := NewInstanceIDSet(request.Instance.Dependencies)

	instance := request.Instance
	if success, err := s.preAcceptInstanceUnsafe(instance); err != nil {
		return nil, err
	} else if !success {
		panic("handling previously seen instance not handled yet")
	}

	// check agreement on seq and deps with leader
	newDeps := NewInstanceIDSet(instance.Dependencies)
	instance.dependencyMatch = extSeq == instance.Sequence && extDeps.Equal(newDeps)

	if err := s.Persist(); err != nil {
		return nil, err
	}

	missingDeps := newDeps.Subtract(extDeps)
	reply := &PreAcceptResponse{
		Accepted:         true,
		MaxBallot:        instance.MaxBallot,
		Instance:         instance,
		MissingInstances: make([]*Instance, 0, len(missingDeps)),
	}

	for iid := range missingDeps {
		inst := s.instances[iid]
		if inst != nil {
			reply.MissingInstances = append(reply.MissingInstances, inst)
		}
	}

	return reply, nil
}

