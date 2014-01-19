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
func (s *Scope) commitInstanceUnsafe(instance *Instance) (bool, error) {
	if existing, exists := s.instances[instance.InstanceID]; exists {
		if existing.Status >= INSTANCE_COMMITTED {
			return false, nil
		} else {
			// this replica may have missed an accept message
			// so copy the seq & deps onto the existing instance
			existing.Status = INSTANCE_COMMITTED
			existing.Dependencies = instance.Dependencies
			existing.Sequence = instance.Sequence
			existing.MaxBallot = instance.MaxBallot
		}
	} else {
		s.instances.Add(instance)
	}

	instance.Status = INSTANCE_COMMITTED
	s.inProgress.Remove(instance)
	s.committed.Add(instance)

	if instance.Sequence > s.maxSeq {
		s.maxSeq = instance.Sequence
	}

	instance.executeTimeout = makeExecuteTimeout()

	if err := s.Persist(); err != nil {
		return false, err
	}

	// wake up any goroutines waiting on this instance,
	// and remove the conditional from the notify map
	if cond, ok := s.commitNotify[instance.InstanceID]; ok {
		cond.Broadcast()
		delete(s.commitNotify, instance.InstanceID)
	}

	return true, nil
}

// sets the given instance as committed
// in the case of handling messages from leaders to replicas
// the message instance should be passed in. It will either
// update the existing instance in place, or add the message
// instance to the scope's instance
// returns a bool indicating that the instance was actually
// accepted (and not skipped), and an error, if applicable
func (s *Scope) commitInstance(instance *Instance) (bool, error) {
	s.lock.Lock()
	s.lock.Unlock()

	return s.commitInstanceUnsafe(instance)
}

// Mark the instance as committed locally, persist state to disk, then send
// commit messages to the other replicas
// We're not concerned with whether the replicas actually respond because, since
// a quorum of nodes have agreed on the dependency graph for this instance, they
// won't be able to do anything else without finding out about it. This method
// will only return an error if persisting the committed state fails
func (s *Scope) sendCommit(instance *Instance, replicas []node.Node) error {
	msg := &CommitRequest{Scope: s.name, Instance: instance}
	sendCommit := func(n node.Node) { n.SendMessage(msg) }
	for _, replica := range replicas {
		go sendCommit(replica)
	}
	return nil
}

func (s *Scope) commitPhase(instance *Instance) error {
	replicas := s.manager.getScopeReplicas(s)

	if success, err := s.commitInstance(instance); err != nil {
		return err
	} else if !success {
		// instance was already committed
		panic("instance status is greater than commit")
	}
	if err := s.sendCommit(instance, replicas); err != nil {
		return err
	}
	return nil
}

// handles an commit message from the command leader for an instance
// this executes the replica commit phase for the given instance
func (s *Scope) HandleCommit(request *CommitRequest) (*CommitResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if success, err := s.commitInstanceUnsafe(request.Instance); err != nil {
		return nil, err
	} else if !success {
		panic("handling previously seen instance not handled yet")
	}

	// asynchronously apply mutation
	// TODO: go s.executeInstance(s.instances[request.Instance.InstanceID])

	return &CommitResponse{}, nil
}

