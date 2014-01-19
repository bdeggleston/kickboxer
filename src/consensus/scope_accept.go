package consensus

import (
	"time"
)

import (
	"node"
)

// sets the given instance as accepted
// in the case of handling messages from leaders to replicas
// the message instance should be passed in. It will either
// update the existing instance in place, or add the message
// instance to the scope's instance
// returns a bool indicating that the instance was actually
// accepted (and not skipped), and an error, if applicable
func (s *Scope) acceptInstanceUnsafe(instance *Instance) (bool, error) {
	if existing, exists := s.instances[instance.InstanceID]; exists {
		if existing.Status >= INSTANCE_ACCEPTED {
			return false, nil
		} else {
			existing.Status = INSTANCE_ACCEPTED
			existing.Dependencies = instance.Dependencies
			existing.Sequence = instance.Sequence
			existing.MaxBallot = instance.MaxBallot
		}
	} else {
		s.instances.Add(instance)
	}

	instance.Status = INSTANCE_ACCEPTED
	instance.commitTimeout = makeAcceptCommitTimeout()
	s.inProgress.Add(instance)

	if instance.Sequence > s.maxSeq {
		s.maxSeq = instance.Sequence
	}

	if err := s.Persist(); err != nil {
		return false, err
	}
	return true, nil
}

// sets the given instance as accepted
// in the case of handling messages from leaders to replicas
// the message instance should be passed in. It will either
// update the existing instance in place, or add the message
// instance to the scope's instance
// returns a bool indicating that the instance was actually
// accepted (and not skipped), and an error, if applicable
func (s *Scope) acceptInstance(instance *Instance) (bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.acceptInstanceUnsafe(instance)
}

func (s *Scope) sendAccept(instance *Instance, replicas []node.Node) error {
	oldTimeout := PREACCEPT_TIMEOUT
	PREACCEPT_TIMEOUT = 50
	defer func() { PREACCEPT_TIMEOUT = oldTimeout }()

	// send the message
	recvChan := make(chan *AcceptResponse, len(replicas))
	msg := &AcceptRequest{Scope: s.name, Instance: instance}
	sendMsg := func(n node.Node) {
		if response, err := n.SendMessage(msg); err != nil {
			logger.Warning("Error receiving PreAcceptResponse: %v", err)
		} else {
			if accept, ok := response.(*AcceptResponse); ok {
				recvChan <- accept
			} else {
				logger.Warning("Unexpected Accept response type: %T", response)
			}
		}
	}
	for _, replica := range replicas {
		go sendMsg(replica)
	}

	// receive the replies
	numReceived := 1 // this node counts as a response
	quorumSize := ((len(replicas) + 1) / 2) + 1
	timeoutEvent := time.After(time.Duration(ACCEPT_TIMEOUT) * time.Millisecond)
	var response *AcceptResponse
	responses := make([]*AcceptResponse, 0, len(replicas))
	for numReceived < quorumSize {
		select {
		case response = <-recvChan:
			responses = append(responses, response)
			numReceived++
		case <-timeoutEvent:
			return NewTimeoutError("Timeout while awaiting accept responses")
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
		// TODO: figure out what to do here. Try again?
		panic("rejected accept not handled yet")
	}
	return nil
}

func (s *Scope) acceptPhase(instance *Instance) error {
	replicas := s.manager.getScopeReplicas(s)

	if success, err := s.acceptInstance(instance); err != nil {
		return err
	} else if !success {
		// how would this even happen?
		panic("instance already exists")
	}
	if err := s.sendAccept(instance, replicas); err != nil {
		return err
	}
	return nil
}

// handles an accept message from the command leader for an instance
// this executes the replica accept phase for the given instance
func (s *Scope) HandleAccept(request *AcceptRequest) (*AcceptResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if instance, exists := s.instances[request.Instance.InstanceID]; exists {
		if instance.MaxBallot >= request.Instance.MaxBallot {
			return &AcceptResponse{Accepted: false, MaxBallot: instance.MaxBallot}, nil
		}
	}

	if success, err := s.acceptInstanceUnsafe(request.Instance); err != nil {
		return nil, err
	} else if !success {
		panic("handling previously seen instance not handled yet")
	}

	if len(request.MissingInstances) > 0 {
		s.addMissingInstancesUnsafe(request.MissingInstances...)
	}

	if err := s.Persist(); err != nil {
		return nil, err
	}
	return &AcceptResponse{Accepted: true}, nil
}

