package consensus

import (
	"time"
)

import (
	"node"
)

func makeAcceptCommitTimeout() time.Time {
	waitTime := ACCEPT_COMMIT_TIMEOUT
	return time.Now().Add(time.Duration(waitTime) * time.Millisecond)
}

// sets the given instance as accepted
// in the case of handling messages from leaders to replicas
// the message instance should be passed in. It will either
// update the existing instance in place, or add the message
// instance to the scope's instance
// returns a bool indicating that the instance was actually
// accepted (and not skipped), and an error, if applicable
func (s *Scope) acceptInstanceUnsafe(inst *Instance, incrementBallot bool) error {
	var instance *Instance
	if existing, exists := s.instances[inst.InstanceID]; exists {
		if existing.Status > INSTANCE_ACCEPTED {
			logger.Debug("Accept: Can't accept instance %v with status %v", inst.InstanceID, inst.Status)
			return NewInvalidStatusUpdateError(existing, INSTANCE_ACCEPTED)
		} else {
			logger.Debug("Accept: accepting existing instance %v", inst.InstanceID)
			existing.Dependencies = inst.Dependencies
			existing.Sequence = inst.Sequence
			existing.MaxBallot = inst.MaxBallot
			existing.Noop = inst.Noop
		}
		instance = existing
	} else {
		logger.Debug("Accept: accept new instance %v", inst.InstanceID)
		instance = inst
	}

	instance.Status = INSTANCE_ACCEPTED
	instance.commitTimeout = makeAcceptCommitTimeout()
	if incrementBallot {
		instance.MaxBallot++
	}
	s.instances.Add(instance)
	s.inProgress.Add(instance)

	if instance.Sequence > s.maxSeq {
		s.maxSeq = instance.Sequence
	}

	if err := s.Persist(); err != nil {
		return err
	}
	logger.Debug("Accept: success for Instance: %+v", *inst.Commands[0])
	return nil
}

// sets the given instance as accepted
// in the case of handling messages from leaders to replicas
// the message instance should be passed in. It will either
// update the existing instance in place, or add the message
// instance to the scope's instance
// returns a bool indicating that the instance was actually
// accepted (and not skipped), and an error, if applicable
func (s *Scope) acceptInstance(instance *Instance, incrementBallot bool) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.acceptInstanceUnsafe(instance, incrementBallot)
}

func (s *Scope) sendAccept(instance *Instance, replicas []node.Node) error {
	// send the message
	recvChan := make(chan *AcceptResponse, len(replicas))
	instanceCopy, err := s.copyInstanceAtomic(instance)
	if err != nil {
		return err
	}
	// TODO: send missing instances
	msg := &AcceptRequest{Scope: s.name, Instance: instanceCopy}
	sendMsg := func(n node.Node) {
		if response, err := n.SendMessage(msg); err != nil {
			logger.Warning("Error receiving AcceptResponse: %v", err)
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
	timeoutEvent := getTimeoutEvent(time.Duration(ACCEPT_TIMEOUT) * time.Millisecond)
	var response *AcceptResponse
	responses := make([]*AcceptResponse, 0, len(replicas))
	for numReceived < quorumSize {
		select {
		case response = <-recvChan:
			logger.Debug("Accept response received: %v", instance.InstanceID)
			responses = append(responses, response)
			numReceived++
		case <-timeoutEvent:
			logger.Debug("Accept timeout for instance: %v", instance.InstanceID)
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
		logger.Debug("Accept request rejected for instance %v", instance.InstanceID)
		// update max ballot from responses
		bmResponses := make([]BallotMessage, len(responses))
		for i, response := range responses {
			bmResponses[i] = BallotMessage(response)
		}
		s.updateInstanceBallotFromResponses(instance, bmResponses)
		return NewBallotError("Ballot number rejected")
	}
	return nil
}

// assigned to var for testing
var scopeAcceptPhase = func(s *Scope, instance *Instance) error {
	logger.Debug("Accept phase started")
	replicas := s.manager.getScopeReplicas(s)

	if err := s.acceptInstance(instance, true); err != nil {
		if _, ok := err.(InvalidStatusUpdateError); !ok {
			return err
		}
	}
	if err := s.sendAccept(instance, replicas); err != nil {
		return err
	}
	logger.Debug("Accept phase completed")
	return nil
}

func (s *Scope) acceptPhase(instance *Instance) error {
	return scopeAcceptPhase(s, instance)
}

// handles an accept message from the command leader for an instance
// this executes the replica accept phase for the given instance
func (s *Scope) HandleAccept(request *AcceptRequest) (*AcceptResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	logger.Debug("Accept message received for %v, ballot: %v", request.Instance.InstanceID, request.Instance.MaxBallot)

	if len(request.MissingInstances) > 0 {
		logger.Debug("Accept: adding %v missing instances", len(request.MissingInstances))
		s.addMissingInstancesUnsafe(request.MissingInstances...)
	}

	if instance, exists := s.instances[request.Instance.InstanceID]; exists {
		if instance.MaxBallot >= request.Instance.MaxBallot {
			logger.Debug("Accept message for %v rejected, %v >= %v", request.Instance.InstanceID, instance.MaxBallot, request.Instance.MaxBallot)
			return &AcceptResponse{Accepted: false, MaxBallot: instance.MaxBallot}, nil
		}
	}

	if err := s.acceptInstanceUnsafe(request.Instance, false); err != nil {
		if _, ok := err.(InvalidStatusUpdateError); !ok {
			return nil, err
		}
	}

	if err := s.Persist(); err != nil {
		return nil, err
	}
	logger.Debug("Accept message replied for %v, accepted", request.Instance.InstanceID)
	return &AcceptResponse{Accepted: true}, nil
}

