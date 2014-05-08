package consensus

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

import (
	"store"
)

// sorts the strongly connected subgraph components
type iidSorter struct {
	depMap map[InstanceID]*Instance
	iids []InstanceID
}

func (i *iidSorter) Len() int {
	return len(i.iids)
}

// returns true if the item at index x is less than
// the item and index y
func (i *iidSorter) Less(x, y int) bool {
	i0 := i.depMap[i.iids[x]]
	i1 := i.depMap[i.iids[y]]

	// first check the embedded timestamp
	t0 := i0.InstanceID.Time()
	t1 := i1.InstanceID.Time()
	if t0 != t1 {
		return t0 < t1
	} else {
		// finally the lexicographic comparison
		return bytes.Compare(i0.InstanceID.Bytes(), i1.InstanceID.Bytes()) == -1
	}
	return false
}

// switches the position of nodes at indices i & j
func (i *iidSorter) Swap(x, y int) {
	i.iids[x], i.iids[y] = i.iids[y], i.iids[x]
}

// topologically sorts instance dependencies, grouped by strongly
// connected components
func (m *Manager) getExecutionOrder(instance *Instance) (exOrder []InstanceID, uncommitted []InstanceID, err error) {
	start := time.Now()
	defer m.statsTiming("execute.dependencies.order.time", start)

	// build a directed graph
	targetDeps := instance.getDependencies()
	targetDepSet := NewInstanceIDSet(targetDeps)

	// allocating 0 length maps greatly reduced the number of GC pauses
	// and sped up execution by 15-18%
	depMap := make(map[InstanceID]*Instance)
	depGraph := make(map[InstanceID][]InstanceID)
	depMap = m.instances.GetMap(depMap, targetDeps)

	uncommittedSet := NewInstanceIDSet([]InstanceID{})

	requiredInstances := NewInstanceIDSet([]InstanceID{})

	var addInstance func(*Instance) error
	addInstance = func(inst *Instance) error {

		// the status and dependencies must be recorded in the same
		// lock context. Otherwise, we'll get uncommitted deps, the
		// instance will be committed with new deps, and our dep graph
		// will be inaccurate in ~0.001% of queries.
		inst.lock.RLock()
		deps := inst.Dependencies
		status := inst.Status
		stronglyConnected := inst.StronglyConnected
		inst.lock.RUnlock()

		depMap = m.instances.GetMap(depMap, deps)

		// if the instance is already executed, and it's not a dependency
		// of the target execution instance, only add it to the dep graph
		// if it's connected to an uncommitted instance, since that will
		// make it part of a strongly connected component of at least one
		// unexecuted instance, and will therefore affect the execution
		// ordering
		if status == INSTANCE_EXECUTED {
			if !targetDepSet.Contains(inst.InstanceID) {
				connected := false
				for _, dep := range deps {
					_, exists := depGraph[dep]
					notExecuted := depMap[dep].getStatus() < INSTANCE_EXECUTED
					targetDep := targetDepSet.Contains(dep)
					required := requiredInstances.Contains(inst.InstanceID)
					connected = connected || exists || notExecuted || targetDep || required
				}
				if !connected {
					return nil
				}
			}
		} else if status < INSTANCE_COMMITTED {
			// record uncomitted instances
			uncommittedSet.Add(inst.InstanceID)
		}

		// add strongly connected components to
		// the set of required instances
		requiredInstances.Combine(stronglyConnected)

		depGraph[inst.InstanceID] = deps
		for _, iid := range deps {
			// don't add instances that are already in the graph,
			// strongly connected instances would create an infinite loop
			if _, exists := depGraph[iid]; exists {
				continue
			}

			inst := depMap[iid]
			if inst == nil {
				return fmt.Errorf("getExecutionOrder: Unknown instance id: %v", iid)
			}
			if err := addInstance(inst); err != nil {
				return err
			}
		}
		return nil
	}

	prepStart := time.Now()
	if err := addInstance(instance); err != nil {
		return nil, nil, err
	}
	m.statsTiming("execute.dependencies.order.sort.prep.time", prepStart)

	// identify the strongly connected components and reverse topologically sort them
	sortStart := time.Now()
	tSorted := tarjanConnect(depGraph)
	m.statsTiming("execute.dependencies.order.sort.tarjan.time", sortStart)
	subSortStart := time.Now()

	// record the strongly connected components on the instances.
	// As instances are executed, they will stop being added to the depGraph for sorting.
	// However, if an instance that's not added to the dep graph is part of a strongly
	// connected component, it can affect the execution order by breaking the component.
	recordSCC := func(inst *Instance, scc []InstanceID) error {
		inst.lock.Lock()
		defer inst.lock.Unlock()
		// TODO: just use an array
		inst.StronglyConnected = NewInstanceIDSet(scc)
		if err := m.Persist(); err != nil {
			return err
		}
		return nil
	}

	hasUncommitted := uncommittedSet.Size() > 0
	exOrder = make([]InstanceID, 0, len(depGraph))
	for _, scc := range tSorted {
		sorter := &iidSorter{depMap: depMap, iids:scc}
		sort.Sort(sorter)
		exOrder = append(exOrder, sorter.iids...)

		// record components only if there's more than one,
		// and there are no uncommitted dependencies
		if len(scc) > 1 && !hasUncommitted {
			for _, iid := range scc {
				if err := recordSCC(depMap[iid], scc); err != nil {
					return nil, nil, err
				}
			}
		}
	}
	m.statsTiming("execute.dependencies.order.sort.sub_graph.time", subSortStart)
	m.statsTiming("execute.dependencies.order.sort.time", sortStart)

	// TODO: will this ever actually find nil instances? they're checked for in the addInstance function
	// and doing this here causes a race condition if the node shows up
	for _, iid := range exOrder {
		if depMap[iid] == nil {
			if m.instances.Get(iid) == nil {
				return nil, nil, fmt.Errorf("getExecutionOrder: Unknown instance id: %v", iid)
			}
		}
	}

	if PAXOS_DEBUG {
		exSet := NewInstanceIDSet(exOrder)
		if diff := requiredInstances.Difference(exSet); diff.Size() > 0 {
			instanceOut := ""
			for _, iid := range diff.List() {
				instanceOut = fmt.Sprintf("%v%v\n", instanceOut, m.instances.Get(iid))
			}
			logger.Critical(fmt.Sprintf("execution order is missing required instances:\n%v\n%v", diff, instanceOut))
		}
	}

	return exOrder, uncommittedSet.List(), nil
}

// executes an instance against the store
func (m *Manager) applyInstance(instance *Instance) (store.Value, error) {
	start := time.Now()
	defer m.statsTiming("execute.instance.apply.time", start)
	m.statsInc("execute.instance.apply.count", 1)

	// lock both
	synchronizedApply := func() (store.Value, error) {
		instance.lock.Lock()
		defer instance.lock.Unlock()
		if status := instance.Status; status == INSTANCE_EXECUTED {
			return nil, nil
		} else if status != INSTANCE_COMMITTED {
			return nil, fmt.Errorf("instance not committed")
		}
		var val store.Value
		var err error
		if !instance.Noop {
			val, err = m.cluster.ApplyQuery(
				instance.Command.Cmd,
				instance.Command.Key,
				instance.Command.Args,
				instance.Command.Timestamp,
			)
			if err != nil {
				return nil, err
			}
		} else {
			m.statsInc("execute.instance.noop.count", 1)
		}

		// update manager bookkeeping
		instance.Status = INSTANCE_EXECUTED
		func() {
			m.executedLock.Lock()
			defer m.executedLock.Unlock()

			m.executed = append(m.executed, instance.InstanceID)
		}()
		// remove instance deps from dependency manager
		if err := m.depsMngr.ReportExecuted(instance); err != nil {
			return nil, err
		}
		if err := m.Persist(); err != nil {
			return nil, err
		}
		m.statsInc("execute.instance.success.count", 1)

		// notify listeners of query result
		for _, listener := range instance.ResultListeners {
			listener <- InstanceResult{val:val, err:err}
		}

		return val, err
	}

	val, err := synchronizedApply()
	if err != nil {
		return nil, err
	}

	logger.Debug("Execute: success: %v on %v", instance.InstanceID, m.GetLocalID())
	return val, nil
}

// executes the dependencies up to the given instance
func (m *Manager) executeDependencyChain(iids []InstanceID, target *Instance) error {
	// don't execute instances 'out from under' client requests. Use the
	// execution grace period first, check the leader id, if it'm not this
	// node, go ahead and execute it if it is, wait for the execution timeout
	imap := make(map[InstanceID]*Instance)
	imap = m.instances.GetMap(imap, iids)
	for _, iid := range iids {
		instance := imap[iid]
		switch instance.getStatus() {
		case INSTANCE_COMMITTED:
			//
			if _, err := m.applyInstance(instance); err != nil {
				m.statsInc("execute.instance.error.count", 1)
				return err
			}
		case INSTANCE_EXECUTED:
			continue
		default:
			return fmt.Errorf("Uncommitted dependencies should be handled before calling executeDependencyChain")
		}

		// only execute up to the target instance
		if instance.InstanceID == target.InstanceID {
			break
		}
	}

	return nil
}

var managerExecuteInstance = func(m *Manager, instance *Instance) error {
	start := time.Now()
	defer m.statsTiming("execute.phase.time", start)
	m.statsInc("execute.phase.count", 1)

	logger.Debug("Execute phase started")
	// get dependency instance ids, sorted in execution order, and a list of uncommitted instances
	var exOrder []InstanceID
	var uncommitted []InstanceID
	var err error
	exOrder, uncommitted, err = m.getExecutionOrder(instance)
	if err != nil {
		return err
	}

	for len(uncommitted) > 0 {
		logger.Info("Execute, %v uncommitted on %v: %+v", len(uncommitted), m.GetLocalID(), uncommitted)
		wg := sync.WaitGroup{}
		wg.Add(len(uncommitted))
		errors := make(chan error, len(uncommitted))

		uncommittedMap := m.instances.GetMap(nil, uncommitted)
		prepare := func(inst *Instance) {
			var err error
			var success bool
			var ballotErr bool
			var commitEvent <- chan bool
			attemptPrepare:
				for i:=0; i<BALLOT_FAILURE_RETRIES; i++ {
					prepareStart := time.Now()
					defer m.statsTiming("execute.phase.prepare.time", prepareStart)
					m.statsInc("execute.phase.prepare.count", 1)

					ballotErr = false
					if err = m.preparePhase(inst); err != nil {
						if _, ok := err.(BallotError); ok {
							// refresh the local instance pointer and
							// assign the commit event so a goroutine is not
							// spun up for each attempt
							inst = m.instances.Get(inst.InstanceID)
							if commitEvent == nil {
								commitEvent = inst.getCommitEvent().getChan()
							}

							logger.Info("Prepare failed with BallotError, waiting to try again")
							ballotErr = true

							// wait on broadcast event or timeout
							waitTime := BALLOT_FAILURE_WAIT_TIME * uint64(i + 1)
							waitTime += uint64(rand.Uint32()) % (waitTime / 2)
							logger.Info("Prepare failed with BallotError, waiting for %v ms to try again", waitTime)
							timeoutEvent := getTimeoutEvent(time.Duration(waitTime) * time.Millisecond)
							select {
							case <- commitEvent:
								// another goroutine committed
								// the instance
								success = true
								break attemptPrepare
							case <-timeoutEvent:
								// continue with the prepare
							}

						} else {
							m.statsInc("execute.phase.prepare.error", 1)
							logger.Warning("Execute prepare failed with error: %v", err)
							errors <- err
							break attemptPrepare
						}
					} else {
						success = true
						break attemptPrepare
					}
				}
			if !success && ballotErr {
				errors <- fmt.Errorf("Prepare failed to commit instance in %v tries", BALLOT_FAILURE_RETRIES)
			}
			wg.Done()
		}
		for _, iid := range uncommitted {
			go prepare(uncommittedMap[iid])
		}
		wg.Wait()

		// catch any errors, if any
		var err error
		select {
		case err = <- errors:
			return err
		default:
			// everything's ok, continue
		}

		logger.Debug("Recalculating dependency chain for %v on %v", instance.InstanceID, m.GetLocalID())
		// if the prepare phase came across instances
		// that no other replica was aware of, it would
		// have run a preaccept phase for it, changing the
		// dependency chain, so the exOrder and uncommitted
		// list need to be updated before continuing
		exOrder, uncommitted, err = m.getExecutionOrder(instance)
		if err != nil {
			return err
		}
	}

	logger.Debug("Executing dependency chain")
	err = m.executeDependencyChain(exOrder, instance)
	if err != nil {
		logger.Error("Execute: executeDependencyChain: %v", err)
	}
	logger.Debug("Execution phase completed")
	return err
}

// applies an instance to the store
// first it will resolve all dependencies, then wait for them to commit/reject, or force them
// to do one or the other. Then it will execute it's committed dependencies, then execute itself
func (m *Manager) executeInstance(instance *Instance) error {
	return managerExecuteInstance(m, instance)
}

