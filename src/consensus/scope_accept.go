package consensus

import (
	"time"
	"math/rand"
)

import (
	"node"
)

func makeAcceptCommitTimeout() time.Time {
	waitTime := ACCEPT_COMMIT_TIMEOUT
	waitTime += uint64(rand.Int63()) % (ACCEPT_COMMIT_TIMEOUT / 10)
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
	return s.acceptInstance(inst, incrementBallot)
}

// sets the given instance as accepted
// in the case of handling messages from leaders to replicas
// the message instance should be passed in. It will either
// update the existing instance in place, or add the message
// instance to the scope's instance
// returns a bool indicating that the instance was actually
// accepted (and not skipped), and an error, if applicable
func (s *Scope) acceptInstance(inst *Instance, incrementBallot bool) error {
	instance, existed := s.getOrSetInstance(inst)

	if existed {
		logger.Debug("Accept: accepting existing instance %v", inst.InstanceID)
	} else {
		logger.Debug("Accept: accepting new instance %v", inst.InstanceID)
		s.statsInc("preaccept.new_instance", 1)
	}

	if err := instance.accept(inst, incrementBallot); err != nil {
		return err
	}

	s.inProgress.Add(instance)
	s.updateSeq(instance.getSeq())
	if err := s.Persist(); err != nil {
		return err
	}

	logger.Debug("Accept: success for Instance: %v", instance.InstanceID)
	return nil
}

func (s *Scope) sendAccept(instance *Instance, replicas []node.Node) error {
	start := time.Now()
	defer s.statsTiming("accept.message.receive", start)

	// send the message
	recvChan := make(chan *AcceptResponse, len(replicas))
	instanceCopy, err := instance.Copy()
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
			s.statsInc("accept.message.receive.timeout", 1)
			logger.Debug("Accept timeout for instance: %v", instance.InstanceID)
			return NewTimeoutError("Timeout while awaiting accept responses")
		}
	}
	logger.Debug("Accept response quorum received: %v", instance.InstanceID)

	// check if any of the messages were rejected
	accepted := true
	for _, response := range responses {
		accepted = accepted && response.Accepted
	}

	// handle rejected pre-accept messages
	if !accepted {
		s.statsInc("accept.message.receive.rejected", 1)
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
	start := time.Now()
	defer s.statsTiming("accept.phase", start)

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
	s.statsInc("accept.message.received", 1)
	start := time.Now()
	defer s.statsTiming("accept.message.response", start)

	logger.Debug("Accept message received for %v, ballot: %v", request.Instance.InstanceID, request.Instance.MaxBallot)

	if len(request.MissingInstances) > 0 {
		logger.Debug("Accept: adding %v missing instances", len(request.MissingInstances))
		s.addMissingInstancesUnsafe(request.MissingInstances...)
	}

	// should the ballot even matter here? If we're receiving an accept response,
	// it means that a quorum of preaccept responses were received by a node
	if instance := s.instances.Get(request.Instance.InstanceID); instance != nil {
		if instance.MaxBallot >= request.Instance.MaxBallot {
			s.statsInc("accept.message.rejected", 1)
			logger.Debug("Accept message for %v rejected, %v >= %v", request.Instance.InstanceID, instance.MaxBallot, request.Instance.MaxBallot)
			return &AcceptResponse{Accepted: false, MaxBallot: instance.MaxBallot}, nil
		}
	}

	if err := s.acceptInstanceUnsafe(request.Instance, false); err != nil {
		if _, ok := err.(InvalidStatusUpdateError); !ok {
			s.statsInc("accept.message.error", 1)
			return nil, err
		}
	}

	if err := s.Persist(); err != nil {
		return nil, err
	}
	logger.Debug("Accept message replied for %v, accepted", request.Instance.InstanceID)
	return &AcceptResponse{Accepted: true}, nil
}

