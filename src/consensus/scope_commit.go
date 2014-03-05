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
// instance to the scope's instance
// returns a bool indicating that the instance was actually
// accepted (and not skipped), and an error, if applicable
func (s *Scope) commitInstanceUnsafe(inst *Instance, incrementBallot bool) error {
	return s.commitInstance(inst, incrementBallot)
}

// sets the given instance as committed
// in the case of handling messages from leaders to replicas
// the message instance should be passed in. It will either
// update the existing instance in place, or add the message
// instance to the scope's instance
// returns a bool indicating that the instance was actually
// accepted (and not skipped), and an error, if applicable
func (s *Scope) commitInstance(inst *Instance, incrementBallot bool) error {
	start := time.Now()
	defer s.statsTiming("commit.instance.time", start)
	s.statsInc("commit.instance.count", 1)
	instance, existed := s.getOrSetInstance(inst)

	if existed {
		logger.Debug("Commit: committing existing instance %v", inst.InstanceID)
	} else {
		logger.Debug("Commit: committing new instance %v", inst.InstanceID)
		s.statsInc("commit.instance.new", 1)
	}

	if err := instance.commit(inst, incrementBallot); err != nil {
		s.statsInc("commit.instance.error", 1)
		return err
	}

	s.updateSeq(instance.getSeq())

	// see preaccept for details on this lock
	s.depsLock.Lock()
	defer s.depsLock.Unlock()

	// update scope bookeeping
	s.inProgress.Remove(instance)
	s.instances.Add(instance)
	s.committed.Add(instance)

	if err := s.Persist(); err != nil {
		s.statsInc("commit.instance.error", 1)
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
func (s *Scope) sendCommit(instance *Instance, replicas []node.Node) error {
	start := time.Now()
	defer s.statsTiming("commit.message.send.time", start)
	s.statsInc("commit.message.send.count", 1)

	instanceCopy, err := instance.Copy()
	if err != nil {
		return err
	}
	msg := &CommitRequest{Scope: s.name, Instance: instanceCopy}
	sendCommitMessage := func(n node.Node) { n.SendMessage(msg) }
	for _, replica := range replicas {
		go sendCommitMessage(replica)
	}
	return nil
}

var scopeCommitPhase = func(s *Scope, instance *Instance) error {
	start := time.Now()
	defer s.statsTiming("commit.phase.time", start)
	s.statsInc("commit.phase.count", 1)

	s.debugInstanceLog(instance, "Commit phase started")
	replicas := s.manager.getScopeReplicas(s)

	if err := s.commitInstance(instance, true); err != nil {
		if _, ok := err.(InvalidStatusUpdateError); !ok {
			return err
		}
	}

	// the given instance is out of date if it was new to this
	// node, switch over to the local instance
	instance = s.instances.Get(instance.InstanceID)

	if err := s.sendCommit(instance, replicas); err != nil {
		return err
	}
	s.debugInstanceLog(instance, "Commit phase completed")
	return nil
}

func (s *Scope) commitPhase(instance *Instance) error {
	return scopeCommitPhase(s, instance)
}

// handles an commit message from the command leader for an instance
// this executes the replica commit phase for the given instance
func (s *Scope) HandleCommit(request *CommitRequest) (*CommitResponse, error) {
	s.statsInc("commit.message.received", 1)
	start := time.Now()
	defer s.statsTiming("commit.message.response.time", start)

	logger.Debug("Commit message received, ballot: %v", request.Instance.MaxBallot)

	if err := s.commitInstanceUnsafe(request.Instance, false); err != nil {
		if _, ok := err.(InvalidStatusUpdateError); !ok {
			s.statsInc("commit.message.response.error", 1)
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

