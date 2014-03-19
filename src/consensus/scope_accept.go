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
func (m *Manager) acceptInstanceUnsafe(inst *Instance, incrementBallot bool) error {
	return m.acceptInstance(inst, incrementBallot)
}

// sets the given instance as accepted
// in the case of handling messages from leaders to replicas
// the message instance should be passed in. It will either
// update the existing instance in place, or add the message
// instance to the scope's instance
// returns a bool indicating that the instance was actually
// accepted (and not skipped), and an error, if applicable
func (m *Manager) acceptInstance(inst *Instance, incrementBallot bool) error {
	start := time.Now()
	defer m.statsTiming("accept.instance.time", start)
	m.statsInc("accept.instance.count", 1)

	instance, existed := m.getOrSetInstance(inst)

	if existed {
		logger.Debug("Accept: accepting existing instance %v", inst.InstanceID)
	} else {
		logger.Debug("Accept: accepting new instance %v", inst.InstanceID)
		m.statsInc("accept.instance.new", 1)
	}

	if err := instance.accept(inst, incrementBallot); err != nil {
		m.statsInc("accept.instance.error", 1)
		return err
	}

	m.inProgress.Add(instance)
	m.updateSeq(instance.getSeq())
	if err := m.Persist(); err != nil {
		m.statsInc("accept.instance.error", 1)
		return err
	}

	logger.Debug("Accept: success for Instance: %v", instance.InstanceID)
	return nil
}

func (m *Manager) sendAccept(instance *Instance, replicas []node.Node) error {
	start := time.Now()
	defer m.statsTiming("accept.message.send.time", start)
	m.statsInc("accept.message.send.count", 1)

	// send the message
	recvChan := make(chan *AcceptResponse, len(replicas))
	instanceCopy, err := instance.Copy()
	if err != nil {
		return err
	}
	// TODO: send missing instances
	msg := &AcceptRequest{Instance: instanceCopy}
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
//	quorumSize := ((len(replicas) + 1) / 2) + 1
	quorumSize := (len(replicas) / 2) + 1
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
			m.statsInc("accept.message.send.timeout", 1)
			logger.Info("Accept timeout for instance: %v", instance.InstanceID)
			return NewTimeoutError("Timeout while awaiting accept responses")
		}
	}
	logger.Debug("Accept response quorum received: %v", instance.InstanceID)

	// check if any of the messages were rejected
	accepted := true
	for _, response := range responses {
		accepted = accepted && response.Accepted
	}

	// handle rejected accept messages
	if !accepted {
		m.statsInc("accept.message.send.rejected", 1)
		logger.Info("Accept request rejected for instance %v", instance.InstanceID)
		// update max ballot from responses
		bmResponses := make([]BallotMessage, len(responses))
		for i, response := range responses {
			bmResponses[i] = BallotMessage(response)
		}
		m.updateInstanceBallotFromResponses(instance, bmResponses)
		return NewBallotError("Ballot number rejected")
	}
	return nil
}

// assigned to var for testing
var scopeAcceptPhase = func(m *Manager, instance *Instance) error {
	start := time.Now()
	defer m.statsTiming("accept.phase.time", start)
	m.statsInc("accept.phase.count", 1)

	logger.Debug("Accept phase started")

	replicas := m.getInstanceReplicas(instance)

	if err := m.acceptInstance(instance, true); err != nil {
		if _, ok := err.(InvalidStatusUpdateError); !ok {
			return err
		}
	}

	// the given instance is out of date if it was new to this
	// node, switch over to the local instance
	instance = m.instances.Get(instance.InstanceID)

	if err := m.sendAccept(instance, replicas); err != nil {
		return err
	}
	logger.Debug("Accept phase completed")
	return nil
}

func (m *Manager) acceptPhase(instance *Instance) error {
	return scopeAcceptPhase(m, instance)
}

// handles an accept message from the command leader for an instance
// this executes the replica accept phase for the given instance
func (m *Manager) HandleAccept(request *AcceptRequest) (*AcceptResponse, error) {
	m.statsInc("accept.message.received.count", 1)
	start := time.Now()
	defer m.statsTiming("accept.message.response.time", start)

	logger.Debug("Accept message received for %v, ballot: %v", request.Instance.InstanceID, request.Instance.MaxBallot)

	if len(request.MissingInstances) > 0 {
		logger.Debug("Accept: adding %v missing instances", len(request.MissingInstances))
		m.addMissingInstancesUnsafe(request.MissingInstances...)
	}

	// should the ballot even matter here? If we're receiving an accept response,
	// it means that a quorum of preaccept responses were received by a node
	if instance := m.instances.Get(request.Instance.InstanceID); instance != nil {
		if ballot := instance.getBallot(); ballot >= request.Instance.MaxBallot {
			m.statsInc("accept.message.response.rejected", 1)
			logger.Info("Accept message for %v rejected, %v >= %v", request.Instance.InstanceID, ballot, request.Instance.MaxBallot)
			return &AcceptResponse{Accepted: false, MaxBallot: ballot}, nil
		}
	}

	if err := m.acceptInstance(request.Instance, false); err != nil {
		if _, ok := err.(InvalidStatusUpdateError); !ok {
			m.statsInc("accept.message.response.error", 1)
			return nil, err
		}
	}

	logger.Debug("Accept message replied for %v, accepted", request.Instance.InstanceID)
	return &AcceptResponse{Accepted: true}, nil
}

