package consensus

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"
)

import (
	"github.com/looplab/tarjan"
)

import (
	"store"
	"runtime"
)

// sorts the strongly connected subgraph components
type iidSorter struct {
	scope *Scope
	iids []InstanceID
}

func (i *iidSorter) Len() int {
	return len(i.iids)
}

// returns true if the item at index x is less than
// the item and index y
func (i *iidSorter) Less(x, y int) bool {
	i0 := i.scope.instances[i.iids[x]]
	i1 := i.scope.instances[i.iids[y]]

	// first check the sequence#
	if i0.Sequence != i1.Sequence {
		return i0.Sequence < i1.Sequence
	} else {
		// then the embedded timestamp
		t0, _ := i0.InstanceID.UUID().Time()
		t1, _ := i1.InstanceID.UUID().Time()
		if t0 != t1 {
			return t0 < t1
		} else {
			// finally the lexicographic comparison
			return bytes.Compare([]byte(i0.InstanceID), []byte(i1.InstanceID)) == -1
		}
	}
	return false
}

// switches the position of nodes at indices i & j
func (i *iidSorter) Swap(x, y int) {
	i.iids[x], i.iids[y] = i.iids[y], i.iids[x]
}

// topologically sorts instance dependencies, grouped by strongly
// connected components
func (s *Scope) getExecutionOrder(instance *Instance) ([]InstanceID, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	// build a directed graph
	depGraph := make(map[interface {}][]interface {}, len(instance.Dependencies) + 1)
	addInstance := func(inst *Instance) {
		iids := make([]interface {}, len(inst.Dependencies))
		for i, iid := range inst.Dependencies {
			iids[i] = iid
		}
		depGraph[inst.InstanceID] = iids
	}

	// add ALL instances to the dependency graph, there may
	// be instances higher upstream that change the ordering
	// of instances down here.
	// TODO: figure out why that is
	for _, inst := range s.instances {
		addInstance(inst)
	}

	// sort with tarjan's algorithm
	tSorted := tarjan.Connections(depGraph)
	exOrder := make([]InstanceID, 0, len(instance.Dependencies) + 1)
	for _, set := range tSorted {
		iids := make([]InstanceID, len(set))
		for i, iid := range set {
			iids[i] = iid.(InstanceID)
		}
		sorter := &iidSorter{scope:s, iids:iids}
		sort.Sort(sorter)
		exOrder = append(exOrder, sorter.iids...)
	}

	for _, iid := range exOrder {
		if s.instances[iid] == nil {
			return nil, fmt.Errorf("getExecutionOrder: Unknown instance id: %v", iid)
		}
	}

	return exOrder, nil
}

func (s *Scope) getUncommittedInstances(iids []InstanceID) []*Instance {
	s.lock.RLock()
	defer s.lock.RUnlock()
	instances := make([]*Instance, 0)
	for _, iid := range iids {
		instance := s.instances[iid]
		if instance.Status < INSTANCE_COMMITTED {
			instances = append(instances, instance)
		}
	}

	return instances
}

// executes an instance against the store
func (s *Scope) applyInstance(instance *Instance) (store.Value, error) {
	if instance.Status != INSTANCE_COMMITTED {
		return nil, fmt.Errorf("instance not committed")
	}
	var val store.Value
	var err error
	if !instance.Noop {
		for _, instruction := range instance.Commands {
			val, err = s.manager.cluster.ApplyQuery(
				instruction.Cmd,
				instruction.Key,
				instruction.Args,
				instruction.Timestamp,
			)
			if err != nil {
				return nil, err
			}
		}
	}

	// update scope bookkeeping
	instance.Status = INSTANCE_EXECUTED
	delete(s.committed, instance.InstanceID)
	s.executed = append(s.executed, instance.InstanceID)
	if err := s.Persist(); err != nil {
		return nil, err
	}
	s.statExecuteCount++

	// wake up any goroutines waiting on this instance,
	// and remove the conditional from the notify map
	if cond, ok := s.executeNotify[instance.InstanceID]; ok {
		cond.Broadcast()
		delete(s.executeNotify, instance.InstanceID)
	}

	logger.Debug("Execute: success: %v on %v", instance.InstanceID, s.GetLocalID())
	return val, nil
}
// executes the dependencies up to the given instance
func (s *Scope) executeDependencyChain(iids []InstanceID, target *Instance) (store.Value, error) {
	var val store.Value
	var err error

	// applies the instance and unlocks the lock, even if the apply
	// call panics. The lock must be aquired before calling this
	applyAndUnlock := func(instance *Instance) (store.Value, error) {
		defer s.lock.Unlock()
		return s.applyInstance(instance)
	}

	// don't execute instances 'out from under' client requests. Use the
	// execution grace period first, check the leader id, if it's not this
	// node, go ahead and execute it if it is, wait for the execution timeout
	for _, iid := range iids {
		val = nil
		err = nil
		s.lock.Lock()
		instance := s.instances[iid]
		switch instance.Status {
		case INSTANCE_COMMITTED:
			//
			if instance.InstanceID == target.InstanceID {
				// execute
				val, err = applyAndUnlock(instance)
				if err != nil { return nil, err }
				s.statExecuteLocalSuccess++
			} else if instance.LeaderID != s.manager.GetLocalID() {
				// execute
				val, err = applyAndUnlock(instance)
				if err != nil { return nil, err }
				s.statExecuteRemote++
			} else {
				// wait for the execution grace period to end
				if time.Now().After(instance.executeTimeout) {
					val, err = applyAndUnlock(instance)
					if err != nil { return nil, err }
					s.statExecuteLocalTimeout++
				} else {
					// get or create broadcast object
					cond, ok := s.executeNotify[instance.InstanceID]
					if !ok {
						cond = makeConditional()
						s.executeNotify[instance.InstanceID] = cond
					}

					// wait on broadcast event or timeout
					broadcastEvent := make(chan bool)
					go func() {
						cond.L.Lock()
						cond.Wait()
						cond.L.Unlock()
						broadcastEvent <- true
					}()
					timeoutEvent := getTimeoutEvent(instance.executeTimeout.Sub(time.Now()))
					s.lock.Unlock()

					select {
					case <- broadcastEvent:
						// instance was executed by another goroutine
						s.statExecuteLocalSuccessWait++
					case <- timeoutEvent:
						// execution timed out
						s.lock.Lock()

						val, err = applyAndUnlock(instance)
						if err != nil { return nil, err }
						s.statExecuteLocalTimeout++
						s.statExecuteLocalTimeoutWait++
					}
				}
			}
		case INSTANCE_EXECUTED:
			s.lock.Unlock()
			continue
		default:
			return nil, fmt.Errorf("Uncommitted dependencies should be handled before calling executeDependencyChain")
		}

		// only execute up to the target instance
		if instance.InstanceID == target.InstanceID {
			break
		}
	}

	return val, nil
}

var scopeExecuteInstance = func(s *Scope, instance *Instance) (store.Value, error) {
	logger.Debug("Execute phase started")
	// get dependency instance ids, sorted in execution order
	exOrder, err := s.getExecutionOrder(instance)
	if err != nil {
		return nil, err
	}

	// prepare uncommitted instances
	var uncommitted []*Instance
	uncommitted = s.getUncommittedInstances(exOrder)

	getCommitNotify := func(inst *Instance) <- chan bool {
		s.lock.Lock()
		defer s.lock.Unlock()

		// get or create broadcast object
		cond, ok := s.commitNotify[inst.InstanceID]
		if !ok {
			cond = makeConditional()
			s.commitNotify[inst.InstanceID] = cond
		}

		broadcastEvent := make(chan bool)
		go func() {
			cond.L.Lock()
			cond.Wait()
			cond.L.Unlock()
			broadcastEvent <- true
		}()
		runtime.Gosched()
		return broadcastEvent
	}


	for len(uncommitted) > 0 {
		s.debugInstanceLog(instance, "Execute, %v uncommitted: %+v", len(uncommitted), uncommitted)
		wg := sync.WaitGroup{}
		wg.Add(len(uncommitted))
		errors := make(chan error, len(uncommitted))
		prepare := func(inst *Instance) {
			var err error
			var success bool
			var ballotErr bool
			for i:=0; i<BALLOT_FAILURE_RETRIES; i++ {
				ballotErr = false
				if err = s.preparePhase(inst); err != nil {
					if _, ok := err.(BallotError); ok {
						ballotErr = true

						// wait on broadcast event or timeout
						broadcastEvent := getCommitNotify(inst)
						timeoutEvent := getTimeoutEvent(time.Duration(BALLOT_FAILURE_WAIT_TIME) * time.Millisecond)
						select {
						case <-broadcastEvent:
							// another goroutine committed
							// the instance
							success = true
							break
						case <-timeoutEvent:
							// continue with the prepare
						}

					} else {
						errors <- err
						break
					}
				} else {
					success = true
					break
				}
			}
			if !success && ballotErr {
				errors <- fmt.Errorf("Prepare failed to commit instance in %v tries", BALLOT_FAILURE_RETRIES)
			}
			wg.Done()
		}
		for _, inst := range uncommitted {
			go prepare(inst)
		}
		wg.Wait()

		// catch any errors, if any
		var err error
		select {
		case err = <- errors:
			return nil, err
		default:
			// everything's ok, continue
		}

		logger.Debug("Recalculating dependency chain for %v on %v", instance.InstanceID, s.GetLocalID())
		// if the prepare phase came across instances
		// that no other replica was aware of, it would
		// have run a preaccept phase for it, changing the
		// dependency chain, so the exOrder and uncommitted
		// list need to be updated before continuing
		exOrder, err = s.getExecutionOrder(instance)
		if err != nil {
			return nil, err
		}
		uncommitted = s.getUncommittedInstances(exOrder)
	}

	logger.Debug("Executing dependency chain")
	val, err := s.executeDependencyChain(exOrder, instance)
	if err != nil {
		logger.Error("Execute: executeDependencyChain: %v", err)
	}
	logger.Debug("Execution phase completed")
	return val, err
}

// applies an instance to the store
// first it will resolve all dependencies, then wait for them to commit/reject, or force them
// to do one or the other. Then it will execute it's committed dependencies, then execute itself
func (s *Scope) executeInstance(instance *Instance) (store.Value, error) {
	return scopeExecuteInstance(s, instance)
}

