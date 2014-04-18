package consensus

import (
	"time"
)

import (
	"node"
)

func makeExecuteTimeout() time.Time {
	return time.Now().Add(time.Duration(EXECUTE_TIMEOUT) * time.Millisecond)
}

// sets the given instance as committed
// in the case of handling messages from leaders to replicas
// the message instance should be passed in. It will either
// update the existing instance in place, or add the message
// instance to the manager's instance
// returns a bool indicating that the instance was actually
// accepted (and not skipped), and an error, if applicable
func (m *Manager) commitInstanceUnsafe(inst *Instance, incrementBallot bool) error {
	return m.commitInstance(inst, incrementBallot)
}

// sets the given instance as committed
// in the case of handling messages from leaders to replicas
// the message instance should be passed in. It will either
// update the existing instance in place, or add the message
// instance to the manager's instance
// returns a bool indicating that the instance was actually
// accepted (and not skipped), and an error, if applicable
func (m *Manager) commitInstance(inst *Instance, incrementBallot bool) error {
	start := time.Now()
	defer m.statsTiming("commit.instance.time", start)
	m.statsInc("commit.instance.count", 1)
	instance, existed := m.getOrSetInstance(inst)

	if existed {
		logger.Debug("Commit: committing existing instance %v", inst.InstanceID)
	} else {
		logger.Debug("Commit: committing new instance %v", inst.InstanceID)
		m.statsInc("commit.instance.new", 1)
	}

	if err := instance.commit(inst, incrementBallot); err != nil {
		m.statsInc("commit.instance.error", 1)
		return err
	}

	m.updateSeq(instance.getSeq())

	if err := m.depsMngr.ReportAcknowledged(instance); err != nil {
		return err
	}

	if err := m.Persist(); err != nil {
		m.statsInc("commit.instance.error", 1)
		return err
	}

	// wake up any goroutines waiting on this instance
	instance.broadcastCommitEvent()

	logger.Debug("Commit: success for Instance: %v", instance.InstanceID)
	return nil
}

// Mark the instance as committed locally, persist state to disk, then send
// commit messages to the other replicas
// We're not concerned with whether the replicas actually respond because, since
// a quorum of nodes have agreed on the dependency graph for this instance, they
// won't be able to do anything else without finding out about it. This method
// will only return an error if persisting the committed state fails
func (m *Manager) sendCommit(instance *Instance, replicas []node.Node) error {
	start := time.Now()
	defer m.statsTiming("commit.message.send.time", start)
	m.statsInc("commit.message.send.count", 1)

	instanceCopy, err := instance.Copy()
	if err != nil {
		return err
	}
	msg := &CommitRequest{Instance: instanceCopy}
	sendCommitMessage := func(n node.Node) {
		if _, err := n.SendMessage(msg); err != nil {
			logger.Critical("Error sending commit message: %v", err)
		}
	}
	for _, replica := range replicas {
		go sendCommitMessage(replica)
	}
	return nil
}

var managerCommitPhase = func(m *Manager, instance *Instance) error {
	start := time.Now()
	defer m.statsTiming("commit.phase.time", start)
	m.statsInc("commit.phase.count", 1)

	logger.Debug("Commit phase started for %v", instance.InstanceID)
	replicas := m.getInstanceReplicas(instance)

	if err := m.commitInstance(instance, true); err != nil {
		if _, ok := err.(InvalidStatusUpdateError); !ok {
			return err
		}
	}

	// the given instance is out of date if it was new to this
	// node, switch over to the local instance
	instance = m.instances.Get(instance.InstanceID)

	if err := m.sendCommit(instance, replicas); err != nil {
		return err
	}
	logger.Debug("Commit phase completed for %v", instance.InstanceID)
	return nil
}

func (m *Manager) commitPhase(instance *Instance) error {
	return managerCommitPhase(m, instance)
}

// handles an commit message from the command leader for an instance
// this executes the replica commit phase for the given instance
func (m *Manager) HandleCommit(request *CommitRequest) (*CommitResponse, error) {
	m.statsInc("commit.message.received", 1)
	start := time.Now()
	defer m.statsTiming("commit.message.response.time", start)

	logger.Debug("Commit message received, ballot: %v", request.Instance.MaxBallot)

	// TODO: check ballot
//	if instance := s.instances.Get(request.Instance.InstanceID); instance != nil {
//		if ballot := instance.getBallot(); ballot >= request.Instance.MaxBallot {
//			s.statsInc("commit.message.response.rejected", 1)
//			logger.Info("Commit message for %v rejected, %v >= %v", request.Instance.InstanceID, ballot, request.Instance.MaxBallot)
//			return &CommitResponse{}, nil
//		}
//	}

	if err := m.commitInstance(request.Instance, false); err != nil {
		if _, ok := err.(InvalidStatusUpdateError); !ok {
			m.statsInc("commit.message.response.error", 1)
			return nil, err
		}
	} else {
		// asynchronously apply mutation
		// TODO: delay based on munber of inProgress instances
//		go s.executeInstance(s.instances.Get(request.Instance.InstanceID))
	}

	logger.Debug("Commit message replied")
	return &CommitResponse{}, nil
}

