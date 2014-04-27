package consensus

import (
	"time"
)

import (
	"node"
)

func makePreAcceptCommitTimeout() time.Time {
	waitTime := PREACCEPT_COMMIT_TIMEOUT
	return time.Now().Add(time.Duration(waitTime) * time.Millisecond)
}

func (m *Manager) preAcceptInstanceUnsafe(inst *Instance, incrementBallot bool) error {
	return m.preAcceptInstance(inst, incrementBallot)
}

// sets the given instance to preaccepted and updates, deps,
// seq, and commit timeout
// in the case of handling messages from leaders to replicas
// the message instance should be passed in. It will either
// update the existing instance in place, or add the message
// instance to the manager's instance
// returns a bool indicating that the instance was actually
// accepted (and not skipped), and an error, if applicable
func (m *Manager) preAcceptInstance(inst *Instance, incrementBallot bool) error {
	start := time.Now()
	defer m.statsTiming("preaccept.instance.time", start)
	m.statsInc("preaccept.instance.count", 1)

	instance, existed := m.getOrSetInstance(inst)

	if existed {
		logger.Debug("PreAccept: preaccepting existing instance %v", inst.InstanceID)
	} else {
		logger.Debug("PreAccept: preaccepting new instance %v", inst.InstanceID)
		m.statsInc("preaccept.instance.new", 1)
	}

	if err := instance.preaccept(inst, incrementBallot); err != nil {
		m.statsInc("preaccept.instance.error", 1)
		return err
	}

	if err := m.Persist(); err != nil {
		m.statsInc("preaccept.instance.error", 1)
		return err
	}

	logger.Debug("PreAccept: success for Instance: %v", instance.InstanceID)
	return nil
}

// sends pre accept responses to the given replicas, and returns their responses. An error will be returned
// if there are problems, or a quorum of responses were not received within the timeout
func (m *Manager) sendPreAccept(instance *Instance, replicas []node.Node) ([]*PreAcceptResponse, error) {
	start := time.Now()
	defer m.statsTiming("preaccept.message.send.time", start)
	m.statsInc("preaccept.message.send.count", 1)

	recvChan := make(chan *PreAcceptResponse, len(replicas))

	instanceCopy, err := instance.Copy()
	if err != nil {
		return nil, err
	}
	msg := &PreAcceptRequest{Instance: instanceCopy}

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

	for _, replica := range replicas {
		go sendMsg(replica)
	}

	numReceived := 1  // this node counts as a response
//	quorumSize := ((len(replicas) + 1) / 2) + 1
	quorumSize := (len(replicas) / 2) + 1
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
			m.statsInc("preaccept.message.send.timeout", 1)
			logger.Info("PreAccept timeout for instance: %v", instance.InstanceID)
			return nil, NewTimeoutError("Timeout while awaiting pre accept responses")
		}
	}
	logger.Debug("PreAccept response quorum received: %v", instance.InstanceID)

	// check if any of the messages were rejected
	accepted := true
	for _, response := range responses {
		accepted = accepted && response.Accepted
	}

	// handle rejected pre-accept messages
	if !accepted {
		m.statsInc("preaccept.message.send.rejected", 1)
		logger.Info("PreAccept request rejected for instance %v", instance.InstanceID)
		// update max ballot from responses
		bmResponses := make([]BallotMessage, len(responses))
		for i, response := range responses {
			bmResponses[i] = BallotMessage(response)
		}
		m.updateInstanceBallotFromResponses(instance, bmResponses)
		return nil, NewBallotError("Ballot number rejected")
	}

	return responses, nil
}

// merges the attributes from the pre accept responses onto the local instance
// and returns a bool indicating if any changes were made
func (m *Manager) mergePreAcceptAttributes(instance *Instance, responses []*PreAcceptResponse) (bool, error) {
	logger.Debug("Merging preaccept attributes from %v responses", len(responses))
	changes := false
	for i, response := range responses {
		mergeChanges, err := instance.mergeAttributes(response.Instance.Dependencies)
		if err != nil {
			return changes, err
		}
		changes = changes || mergeChanges
		logger.Debug("Merging preaccept attributes from response %v, changes: %v", i+1, mergeChanges)
	}
	if err := m.Persist(); err != nil {
		return true, err
	}
	logger.Debug("Preaccept attributes merged")
	return changes, nil
}

// assigned to var for testing
var managerPreAcceptPhase = func(m *Manager, instance *Instance) (acceptRequired bool, err error) {
	replicas := m.getInstanceReplicas(instance)

	if err := m.preAcceptInstance(instance, true); err != nil {
		// this may be possible during an explicit prepare
		if _, ok := err.(InvalidStatusUpdateError); !ok {
			return false, err
		}
	}

	// the given instance is out of date if it was new to this
	// node, switch over to the local instance
	instance = m.instances.Get(instance.InstanceID)

	// send instance pre-accept to replicas
	paResponses, err := m.sendPreAccept(instance, replicas)
	if err != nil {
		// quorum failed, a later explicit prepare may
		// fix it but nothing can be done now
		return false, err
	}

	// add missing instances
	addMissingInstances := func() error {
//		m.depsLock.Lock()
//		defer m.depsLock.Unlock()
		for _, response := range paResponses {
			if len(response.MissingInstances) > 0 {
				if err := m.addMissingInstancesUnsafe(response.MissingInstances...); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := addMissingInstances(); err != nil {
		return false, err
	}

	return m.mergePreAcceptAttributes(instance, paResponses)
}

// runs the full preaccept phase for the given instance, returning
// a bool indicating if an accept phase is required or not
func (m *Manager) preAcceptPhase(instance *Instance) (acceptRequired bool, err error) {
	start := time.Now()
	defer m.statsTiming("preaccept.phase.time", start)
	m.statsInc("preaccept.phase.count", 1)

	logger.Debug("PreAccept phase started")
	defer logger.Debug("Preaccept phase completed: %v %v", acceptRequired, err)
	return managerPreAcceptPhase(m, instance)
}


// handles a preaccept message from the command leader for an instance
// this executes the replica preaccept phase for the given instance
func (m *Manager) HandlePreAccept(request *PreAcceptRequest) (*PreAcceptResponse, error) {
	m.statsInc("preaccept.message.received.count", 1)
	start := time.Now()
	defer m.statsTiming("preaccept.message.response.time", start)

	logger.Debug("PreAccept message received for %v, ballot: %v", request.Instance.InstanceID, request.Instance.MaxBallot)
	logger.Debug("Processing PreAccept message for %v, ballot: %v", request.Instance.InstanceID, request.Instance.MaxBallot)

	extDeps := NewInstanceIDSet(request.Instance.Dependencies)

	if instance := m.instances.Get(request.Instance.InstanceID); instance != nil {
		if ballot := instance.getBallot(); ballot >= request.Instance.MaxBallot {
			m.statsInc("preaccept.message.response.rejected", 1)
			logger.Info("PreAccept message for %v rejected, %v >= %v", request.Instance.InstanceID, ballot, request.Instance.MaxBallot)
			instCopy, err := instance.Copy()
			if err != nil {
				return nil, err
			}
			return &PreAcceptResponse{Accepted: false, Instance:instCopy, MaxBallot: instCopy.getBallot()}, nil
		}
	}

	instanceStart := time.Now()
	if err := m.preAcceptInstance(request.Instance, false); err != nil {
		if _, ok := err.(InvalidStatusUpdateError); !ok {
			m.statsInc("accept.message.response.error", 1)
			logger.Warning("Error processing PreAccept message for %v, : %v", request.Instance.InstanceID, err)
			return nil, err
		} else {
			logger.Info("InvalidStatusUpdateError processing PreAccept message for %v, : %v", request.Instance.InstanceID, err)
		}
	}
	m.statsTiming("preaccept.message.response.instance.time", instanceStart)

	// check agreement on seq and deps with leader
	instance := m.instances.Get(request.Instance.InstanceID)
	newDeps := NewInstanceIDSet(instance.Dependencies)
	instance.DependencyMatch = extDeps.Equal(newDeps)

	if err := m.Persist(); err != nil {
		return nil, err
	}

	missingStart := time.Now()
	missingDeps := newDeps.Difference(extDeps)
	reply := &PreAcceptResponse{
		Accepted:         true,
		MissingInstances: make([]*Instance, 0, len(missingDeps)),
	}

	if instanceCopy, err := instance.Copy(); err != nil {
		return nil, err
	} else {
		reply.Instance = instanceCopy
		reply.MaxBallot = instanceCopy.MaxBallot
	}

	for iid := range missingDeps {
		m.statsInc("preaccept.message.response.missing.count", 1)
		inst := m.instances.Get(iid)
		if inst != nil {
			if instanceCopy, err := inst.Copy(); err != nil {
				return nil, err
			} else {
				reply.MissingInstances = append(reply.MissingInstances, instanceCopy)
			}
		}
	}
	m.statsTiming("preaccept.message.response.missing.time", missingStart)

	logger.Debug("PreAccept message replied with accepted for %v: %v", request.Instance.InstanceID, reply.Accepted)
	if len(reply.MissingInstances) > 0 {
		logger.Debug("PreAccept reply for %v includes %v missing instances", request.Instance.InstanceID, len(reply.MissingInstances))
	}
	return reply, nil
}

