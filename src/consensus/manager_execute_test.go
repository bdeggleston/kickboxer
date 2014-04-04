/**
tests the execution of committed commands
*/
package consensus

import (
	"fmt"
	"runtime"
	"time"
)

import (
	"launchpad.net/gocheck"
)

import (
	"node"
	"store"
)

type baseExecutionTest struct {
	baseManagerTest
	expectedOrder []InstanceID
	maxIdx int

	oldExecutionTimeout uint64
}

func (s *baseExecutionTest) SetUpSuite(c *gocheck.C) {
	s.oldExecutionTimeout = EXECUTE_TIMEOUT
	EXECUTE_TIMEOUT = uint64(5)
}

func (s *baseExecutionTest) TearDownSuite(c *gocheck.C) {
	EXECUTE_TIMEOUT = s.oldExecutionTimeout
}

// makes a set of interdependent instances, and sets
// their expected ordering
func (s *baseExecutionTest) SetUpTest(c *gocheck.C) {
	s.baseManagerTest.SetUpTest(c)
	s.expectedOrder  = make([]InstanceID, 0)
	lastVal := 0

	// sets up a new instance, and appends it to the expected order needs
	// to be called in the same order as the expected dependency ordering
	addInst := func() *Instance {
		inst := s.manager.makeInstance(s.getInstruction(lastVal))
		inst.Successors = []node.NodeId{inst.LeaderID}
		lastVal++
		s.manager.preAcceptInstanceUnsafe(inst, false)
		s.expectedOrder = append(s.expectedOrder, inst.InstanceID)
		s.maxIdx = len(s.expectedOrder) - 1
		return inst
	}

	// adds additional dependecies to the given instance
	addDeps := func(inst *Instance, deps ...*Instance) {
		for _, dep := range deps {
			inst.Dependencies = append(inst.Dependencies, dep.InstanceID)
		}
	}

	// set 1, interdependent
	i0 := addInst()
	i1 := addInst()
	i2 := addInst()
	addDeps(i0, i1, i2)
	addDeps(i1, i2)

	// set 1, conflicting seq
	i3 := addInst()
	i4 := addInst()
	i5 := addInst()
	addDeps(i3, i4, i5)
	addDeps(i4, i5)
	i3.Sequence = i5.Sequence
	i4.Sequence = i5.Sequence
}


type ExecuteInstanceTest struct {
	baseExecutionTest
	oldPreparePhase func(*Manager, *Instance) error
	oldBallotTimeout uint64
	preparePhaseCalls int

	toPrepare *Instance
	toExecute *Instance
}

var _ = gocheck.Suite(&ExecuteInstanceTest{})

func (s *ExecuteInstanceTest) SetUpSuite(c *gocheck.C) {
	s.baseExecutionTest.SetUpSuite(c)
	s.oldBallotTimeout = BALLOT_FAILURE_WAIT_TIME
	BALLOT_FAILURE_WAIT_TIME = uint64(5)
}

func (s *ExecuteInstanceTest) TearDownSuite(c *gocheck.C) {
	s.baseExecutionTest.TearDownSuite(c)
	BALLOT_FAILURE_WAIT_TIME = s.oldBallotTimeout
}

func (s *ExecuteInstanceTest) SetUpTest(c *gocheck.C) {
	s.baseExecutionTest.SetUpTest(c)
	s.oldPreparePhase = managerPreparePhase
	s.preparePhaseCalls = 0

	// make all instances 'timed out'
	for _, iid := range s.expectedOrder {
		instance := s.manager.instances.Get(iid)
		instance.commitTimeout = time.Now().Add(time.Duration(-1) * time.Millisecond)
	}
	s.toExecute = s.manager.instances.Get(s.expectedOrder[2])
	exOrder, err := s.manager.getExecutionOrder(s.toExecute)
	c.Assert(err, gocheck.IsNil)
	// commit all but the first instance
	for _, iid := range exOrder[1:] {
		instance := s.manager.instances.Get(iid)
		err := s.manager.commitInstance(instance, false)
		c.Assert(err, gocheck.IsNil)
	}
	s.toPrepare = s.manager.instances.Get(exOrder[0])
	c.Assert(len(s.manager.getUncommittedInstances(exOrder)), gocheck.Equals, 1)
}

func (s *ExecuteInstanceTest) TearDownTest(c *gocheck.C) {
	managerPreparePhase = s.oldPreparePhase
}

func (s *ExecuteInstanceTest) patchPreparePhase(err error, commit bool) {
	managerPreparePhase = func(manager *Manager, instance *Instance) error {
		s.preparePhaseCalls++
		if commit {
			manager.commitInstance(instance, false)
		}
		return err
	}
}

// tests that an explicit prepare is initiated on any uncommitted
// instance in the instance's dependency graph
func (s *ExecuteInstanceTest) TestExplicitPrepare(c *gocheck.C) {
	s.patchPreparePhase(nil, true)
	val, err := s.manager.executeInstance(s.toExecute)
	c.Assert(val, gocheck.NotNil)
	c.Assert(err, gocheck.IsNil)
	c.Assert(s.preparePhaseCalls, gocheck.Equals, 1)

	c.Check(s.toPrepare.Status, gocheck.Equals, INSTANCE_EXECUTED)
	c.Check(s.toExecute.Status, gocheck.Equals, INSTANCE_EXECUTED)

}

// tests that an explicit prepare is retried if it fails due to
// a ballot failure
func (s *ExecuteInstanceTest) TestExplicitPrepareRetry(c *gocheck.C) {
	// die once, succeed on second try
	managerPreparePhase = func(manager *Manager, instance *Instance) error {
		if s.preparePhaseCalls > 0 {
			manager.commitInstance(instance, false)
		} else {
			s.preparePhaseCalls++
			return NewBallotError("nope")
		}
		s.preparePhaseCalls++
		return nil
	}

	val, err := s.manager.executeInstance(s.toExecute)
	c.Assert(val, gocheck.NotNil)
	c.Assert(err, gocheck.IsNil)
	c.Assert(s.preparePhaseCalls, gocheck.Equals, 2)

	c.Check(s.toPrepare.Status, gocheck.Equals, INSTANCE_EXECUTED)
	c.Check(s.toExecute.Status, gocheck.Equals, INSTANCE_EXECUTED)
}

// tests that an explicit prepare which is retried, will wait before
// retrying, but will abort if a commit notify event is broadcasted
func (s *ExecuteInstanceTest) TestExplicitPrepareRetryCondAbort(c *gocheck.C) {
	// skip waiting
	oldBallotFailureWaitTime := BALLOT_FAILURE_WAIT_TIME
	BALLOT_FAILURE_WAIT_TIME = uint64(10000)
	defer func() { BALLOT_FAILURE_WAIT_TIME = oldBallotFailureWaitTime }()

	s.patchPreparePhase(NewBallotError("nope"), false)

	s.toExecute.getExecuteEvent()
	var val store.Value
	var err error
	go func() { val, err = s.manager.executeInstance(s.toExecute) }()
	runtime.Gosched()

	// 'commit' and notify while prepare waits
	c.Assert(s.toPrepare.commitEvent, gocheck.NotNil)
	cerr := s.manager.commitInstance(s.toPrepare, false)
	c.Assert(cerr, gocheck.IsNil)
	s.toPrepare.broadcastCommitEvent()

	s.toExecute.getExecuteEvent().wait()
	c.Assert(val, gocheck.NotNil)
	c.Assert(err, gocheck.IsNil)
	c.Assert(s.preparePhaseCalls, gocheck.Equals, 1)

	c.Check(s.toPrepare.Status, gocheck.Equals, INSTANCE_EXECUTED)
	c.Check(s.toExecute.Status, gocheck.Equals, INSTANCE_EXECUTED)
}

// tests that execute will return an error if a prepare call returns
// a non ballot related error
func (s *ExecuteInstanceTest) TestExplicitPrepareFailure(c *gocheck.C) {
	s.patchPreparePhase(fmt.Errorf("negative"), false)
	c.Log("TestExplicitPrepareFailure")
	val, err := s.manager.executeInstance(s.toExecute)
	c.Assert(val, gocheck.IsNil)
	c.Assert(err, gocheck.NotNil)
	c.Assert(s.preparePhaseCalls, gocheck.Equals, 1)

	c.Check(s.toPrepare.Status, gocheck.Equals, INSTANCE_PREACCEPTED)
	c.Check(s.toExecute.Status, gocheck.Equals, INSTANCE_COMMITTED)
}

// tests that execute will return an error if it receives more ballot
// failures than the BALLOT_FAILURE_RETRIES value
func (s *ExecuteInstanceTest) TestExplicitPrepareBallotFailure(c *gocheck.C) {
	s.patchPreparePhase(NewBallotError("nope"), false)
	val, err := s.manager.executeInstance(s.toExecute)
	c.Assert(val, gocheck.IsNil)
	c.Assert(err, gocheck.NotNil)
	c.Assert(s.preparePhaseCalls, gocheck.Equals, BALLOT_FAILURE_RETRIES)

	c.Check(s.toPrepare.Status, gocheck.Equals, INSTANCE_PREACCEPTED)
	c.Check(s.toExecute.Status, gocheck.Equals, INSTANCE_COMMITTED)
}

// tests that if a prepare phase removes an instance from the target
// instance's dependency graph, it's not executed
func (s *ExecuteInstanceTest) TestPrepareExOrderChange(c *gocheck.C) {
	managerPreparePhase = func(manager *Manager, instance *Instance) error {
		instance.Sequence += 5
		s.preparePhaseCalls++
		manager.commitInstance(instance, false)
		return nil
	}

	val, err := s.manager.executeInstance(s.toExecute)
	for i:=0; i<20; i++ {
		runtime.Gosched()
	}
	c.Assert(val, gocheck.NotNil)
	c.Assert(err, gocheck.IsNil)
	c.Check(s.preparePhaseCalls, gocheck.Equals, 1)

	c.Check(s.toPrepare.Status, gocheck.Equals, INSTANCE_COMMITTED)
	c.Check(s.toExecute.Status, gocheck.Equals, INSTANCE_EXECUTED)
}

type ExecuteDependencyChainTest struct {
	baseExecutionTest
}

var _ = gocheck.Suite(&ExecuteDependencyChainTest{})

// commits all instances
func (s *ExecuteDependencyChainTest) commitInstances() {
	for _, iid := range s.expectedOrder {
		s.manager.commitInstance(s.manager.instances.Get(iid), false)
	}
}

func (s *ExecuteDependencyChainTest) TestDependencyOrdering(c *gocheck.C) {
	s.commitInstances()

	lastInstance := s.manager.instances.Get(s.expectedOrder[len(s.expectedOrder) - 1])
	actual, err := s.manager.getExecutionOrder(lastInstance)
	c.Assert(err, gocheck.IsNil)
	c.Assert(actual, gocheck.DeepEquals, s.expectedOrder)
}

// tests that the dependency ordering of connected instances is the same, regardless of the 'target' instance
func (s *ExecuteDependencyChainTest) TestInstanceDependentConnectedDependencyOrdering(c *gocheck.C) {
	s.commitInstances()

	inst := s.manager.makeInstance(s.getInstruction(6))
	s.manager.preAcceptInstanceUnsafe(inst, false)
	inst.commit(nil, false)
	c.Assert(len(inst.Dependencies), gocheck.Equals, 1)
	s.expectedOrder = append(s.expectedOrder, inst.InstanceID)

	for i, iid := range s.expectedOrder {
		instance := s.manager.instances.Get(iid)
		var expected []InstanceID
		if i > 5 {
			expected = s.expectedOrder
		} else if i > 2{
			expected = s.expectedOrder[:6]
		} else {
			expected = s.expectedOrder[:3]
		}
		actual, err := s.manager.getExecutionOrder(instance)
		c.Assert(err, gocheck.IsNil)
		if c.Check(actual, gocheck.DeepEquals, expected, gocheck.Commentf("iid %v", i)) {
			c.Logf("iid %v, ok", i)
		}
	}
}

// tests that the dependency ordering is the same, regardless of the 'target' instance
func (s *ExecuteDependencyChainTest) TestInstanceDependentDependencyOrdering(c *gocheck.C) {
	s.manager = NewManager(s.manager.cluster)
	instances := make([]*Instance, 50)
	for i := range instances {
		instance := s.manager.makeInstance(s.getInstruction(1))
		s.manager.preAcceptInstance(instance, false)
		numExpectedDeps := 1
		if i == 0 {
			numExpectedDeps = 0
		}
		c.Assert(len(instance.Dependencies), gocheck.Equals, numExpectedDeps, gocheck.Commentf("instance: %v", i))
		s.manager.commitInstance(instance, false)
		instances[i] = instance
	}

	expected := make([]InstanceID, len(instances))
	for i, instance := range instances {
		expected[i] = instance.InstanceID
	}

	for i, instance := range instances {
		actual, err := s.manager.getExecutionOrder(instance)
		c.Assert(err, gocheck.IsNil)
		c.Assert(actual, gocheck.DeepEquals, expected[:i+1], gocheck.Commentf("instance %v", i))
	}
}

// getExecutionOrder should return an error if an instance
// depends on an instance that the manager hasn't seen
func (s *ExecuteDependencyChainTest) TestMissingDependencyOrder(c *gocheck.C) {
	s.commitInstances()

	lastInstance := s.manager.instances.Get(s.expectedOrder[len(s.expectedOrder) - 1])
	lastInstance.Dependencies = append(lastInstance.Dependencies, NewInstanceID())
	actual, err := s.manager.getExecutionOrder(lastInstance)
	c.Assert(actual, gocheck.IsNil)
	c.Assert(err, gocheck.NotNil)
}

// tests that instances up to and including the given
// instance are executed when the dependencies all
// have a remote instance id
func (s *ExecuteDependencyChainTest) TestExternalDependencySuccess(c *gocheck.C) {
	s.commitInstances()
	targetInst := s.manager.instances.Get(s.expectedOrder[s.maxIdx - 1])

	// set non-target dependency leaders to a remote node
	remoteID := node.NewNodeId()
	for i, iid := range s.expectedOrder {
		if i > (s.maxIdx - 2) { break }
		instance := s.manager.instances.Get(iid)
		instance.LeaderID = remoteID
	}

	val, err := s.manager.executeInstance(targetInst)

	c.Assert(err, gocheck.IsNil)
	c.Assert(val, gocheck.NotNil)

	// check stats
	c.Check(s.manager.stats.(*mockStatter).counters["execute.remote.success.count"], gocheck.Equals, int64(4))
	c.Check(s.manager.stats.(*mockStatter).counters["execute.local.success.count"], gocheck.Equals, int64(1))
	c.Check(s.manager.stats.(*mockStatter).counters["execute.instance.apply.count"], gocheck.Equals, int64(5))

	// check returned value
	c.Assert(&intVal{}, gocheck.FitsTypeOf, val)
	c.Check(4, gocheck.Equals, val.(*intVal).value)

	// check the number of instructions
	c.Assert(len(s.cluster.instructions), gocheck.Equals, len(s.expectedOrder) - 1)

	// check all the instances, instructions, etc
	for i:=0; i<len(s.expectedOrder); i++ {
		instance := s.manager.instances.Get(s.expectedOrder[i])

		if i == len(s.expectedOrder) - 1 {
			// unexecuted instance
			c.Check(instance.Status, gocheck.Equals, INSTANCE_COMMITTED)

		} else {
			// executed instances
			c.Check(instance.Status, gocheck.Equals, INSTANCE_EXECUTED)
			instruction := s.cluster.instructions[i]
			c.Check(instruction.Args[0], gocheck.Equals, fmt.Sprint(i))
		}
	}
}

func (s *ExecuteDependencyChainTest) TestRejectedInstanceSkip(c *gocheck.C) {
	s.commitInstances()
	rejectInst := s.manager.instances.Get(s.expectedOrder[0])
	rejectInst.Noop = true
	targetInst := s.manager.instances.Get(s.expectedOrder[1])

	val, err := s.manager.executeInstance(targetInst)

	c.Assert(err, gocheck.IsNil)
	c.Assert(val, gocheck.NotNil)
	c.Check(val.(*intVal).value, gocheck.Equals, 1)

	// check the number of instructions
	c.Assert(len(s.cluster.instructions), gocheck.Equals, 1)
}

// tests the execution of dependency chains when all of the target dependencies
// have are past their execution grace period
func (s *ExecuteDependencyChainTest) TestTimedOutLocalDependencySuccess(c *gocheck.C) {
	s.commitInstances()
	targetInst := s.manager.instances.Get(s.expectedOrder[len(s.expectedOrder) - 2])

	for i, iid := range s.expectedOrder {
		if i > (s.maxIdx - 2) { break }
		instance := s.manager.instances.Get(iid)
		instance.executeTimeout = time.Now().Add(time.Duration(-1) * time.Second)
	}

	val, err := s.manager.executeInstance(targetInst)

	c.Assert(err, gocheck.IsNil)
	c.Assert(val, gocheck.NotNil)

	// check stats
	stats := s.manager.stats.(*mockStatter)
	c.Check(stats.counters["execute.local.timeout.count"], gocheck.Equals, int64(4))
	c.Check(stats.counters["execute.local.timeout.wait.count"], gocheck.Equals, int64(0))
	c.Check(stats.counters["execute.local.success.count"], gocheck.Equals, int64(1))
	c.Check(stats.counters["execute.instance.apply.count"], gocheck.Equals, int64(5))

	// check returned value
	c.Assert(&intVal{}, gocheck.FitsTypeOf, val)
	c.Check(4, gocheck.Equals, val.(*intVal).value)

	// check the number of instructions
	c.Assert(len(s.cluster.instructions), gocheck.Equals, len(s.expectedOrder) - 1)

	// check all the instances, instructions, etc
	for i:=0; i<len(s.expectedOrder); i++ {
		instance := s.manager.instances.Get(s.expectedOrder[i])

		if i == len(s.expectedOrder) - 1 {
			// unexecuted instance
			c.Check(instance.Status, gocheck.Equals, INSTANCE_COMMITTED)

		} else {
			// executed instances
			c.Check(instance.Status, gocheck.Equals, INSTANCE_EXECUTED)
			instruction := s.cluster.instructions[i]
			c.Check(instruction.Args[0], gocheck.Equals, fmt.Sprint(i))
		}
	}
}

// tests that, if a dependency's grace period has not passed, execute dependency chain
// will wait on it's notify cond. If another gorouting does not executes the instance
// before the wait period times out, the called executeDependencyChain will execute it,
// and continue executing instances
func (s *ExecuteDependencyChainTest) TestLocalDependencyTimeoutSuccess(c *gocheck.C) {
	s.commitInstances()
	depInst := s.manager.instances.Get(s.expectedOrder[0])
	depInst.executeTimeout = time.Now().Add(time.Duration(10) * time.Millisecond)
	targetInst := s.manager.instances.Get(s.expectedOrder[1])

	val, err := s.manager.executeInstance(targetInst)

	c.Assert(err, gocheck.IsNil)
	c.Assert(val, gocheck.NotNil)

	// check stats
	stats := s.manager.stats.(*mockStatter)
	c.Check(stats.counters["execute.local.timeout.count"], gocheck.Equals, int64(1))
	c.Check(stats.counters["execute.local.timeout.wait.count"], gocheck.Equals, int64(1))
	c.Check(stats.counters["execute.local.success.count"], gocheck.Equals, int64(1))
	c.Check(stats.counters["execute.instance.apply.count"], gocheck.Equals, int64(2))

	// check the number of instructions
	c.Assert(len(s.cluster.instructions), gocheck.Equals, 2)

	// check all the instances, instructions, etc
	for i:=0; i<2; i++ {
		instance := s.manager.instances.Get(s.expectedOrder[i])

		// executed instances
		c.Check(instance.Status, gocheck.Equals, INSTANCE_EXECUTED)
		instruction := s.cluster.instructions[i]
		c.Check(instruction.Args[0], gocheck.Equals, fmt.Sprint(i))
	}
}

// tests that, if a dependency's grace period has not passed, execute dependency chain
// will wait on it's notify cond. If another gorouting executes the instance before
// the wait period times out, the called executeDependencyChain will skip it, and
// continue executing instances
func (s *ExecuteDependencyChainTest) TestLocalDependencyBroadcastSuccess(c *gocheck.C) {
	s.commitInstances()
	depInst := s.manager.instances.Get(s.expectedOrder[0])
	depInst.executeTimeout = time.Now().Add(time.Duration(1) * time.Minute)
	targetInst := s.manager.instances.Get(s.expectedOrder[1])

	var val store.Value
	var err error

	depInst.getExecuteEvent()

	go func() { val, err = s.manager.executeInstance(targetInst) }()
	runtime.Gosched()  // yield

	// goroutine should be waiting
	c.Check(s.manager.stats.(*mockStatter).counters["execute.instance.apply.count"], gocheck.Equals, int64(0))

	// release wait
	depInst.broadcastExecuteEvent()
	for i:=0; i<20; i++ {
		if val == nil {
			runtime.Gosched()
		} else {
			break
		}
	}

	c.Assert(err, gocheck.IsNil)
	c.Check(val, gocheck.NotNil)

	// check stats
	stats := s.manager.stats.(*mockStatter)
	c.Check(stats.counters["execute.local.wait.event.count"], gocheck.Equals, int64(1))
	c.Check(stats.counters["execute.local.timeout.count"], gocheck.Equals, int64(0))
	c.Check(stats.counters["execute.local.timeout.wait.count"], gocheck.Equals, int64(0))
	c.Check(stats.counters["execute.local.success.count"], gocheck.Equals, int64(1))
	c.Check(stats.counters["execute.instance.apply.count"], gocheck.Equals, int64(1))

	// depInst should not have been executed, by receiving the broadcastEvent,
	// it should have assumed that another goroutine executed the instance
	c.Check(depInst.Status, gocheck.Equals, INSTANCE_COMMITTED)

	// check the number of instructions
	c.Assert(len(s.cluster.instructions), gocheck.Equals, 1)

	// the target instance should have been executed though
	c.Check(targetInst.Status, gocheck.Equals, INSTANCE_EXECUTED)
}

// tests that instances are not executed twice
func (s *ExecuteDependencyChainTest) TestSkipExecuted(c *gocheck.C) {
	s.commitInstances()
	targetInst := s.manager.instances.Get(s.expectedOrder[s.maxIdx])

	// execute all but the target dependency
	for i:=0; i<s.maxIdx; i++ {
		instance := s.manager.instances.Get(s.expectedOrder[i])
		instance.Status = INSTANCE_EXECUTED
	}

	val, err := s.manager.executeInstance(targetInst)

	c.Assert(err, gocheck.IsNil)
	c.Assert(val, gocheck.NotNil)

	// the target instance should have been executed
	c.Check(targetInst.Status, gocheck.Equals, INSTANCE_EXECUTED)

	// and the cluster should have received only one instruction
	c.Assert(len(s.cluster.instructions), gocheck.Equals, 1)
	c.Check(s.cluster.instructions[0].Args[0], gocheck.Equals, fmt.Sprint(s.maxIdx))
}

// tests that an error is returned if an uncommitted instance id is provided
func (s *ExecuteDependencyChainTest) TestUncommittedFailure(c *gocheck.C) {
	targetInst := s.manager.instances.Get(s.expectedOrder[0])

	val, err := s.manager.executeDependencyChain(s.expectedOrder, targetInst)

	c.Assert(err, gocheck.NotNil)
	c.Assert(val, gocheck.IsNil)
}

type ExecuteApplyInstanceTest struct {
	baseManagerTest
}

var _ = gocheck.Suite(&ExecuteApplyInstanceTest{})

func (s *ExecuteApplyInstanceTest) TestSuccess(c *gocheck.C) {
	instance := s.manager.makeInstance(s.getInstruction(5))
	err := s.manager.commitInstance(instance, false)
	c.Assert(err, gocheck.IsNil)
	val, err := s.manager.applyInstance(instance)
	c.Assert(err, gocheck.IsNil)
	c.Assert(val, gocheck.FitsTypeOf, &intVal{})
	c.Assert(val.(*intVal).value, gocheck.Equals, 5)
	c.Check(instance.Status, gocheck.Equals, INSTANCE_EXECUTED)
	c.Check(len(s.cluster.instructions), gocheck.Equals, 1)
	c.Check(s.cluster.values["a"].value, gocheck.Equals, 5)
}

//
func (s *ExecuteApplyInstanceTest) TestSkipRejectedInstance(c *gocheck.C) {
	instance := s.manager.makeInstance(s.getInstruction(5))
	instance.Status = INSTANCE_COMMITTED
	instance.Noop = true
	val, err := s.manager.applyInstance(instance)
	c.Assert(err, gocheck.IsNil)
	c.Assert(val, gocheck.IsNil)
	c.Check(instance.Noop, gocheck.Equals, true)
	c.Check(len(s.cluster.instructions), gocheck.Equals, 0)
}

// tests the executing an instance against the store
// broadcasts to an existing notify instance, and
// removes it from the executeNotify map
func (s *ExecuteApplyInstanceTest) TestNotifyHandling(c *gocheck.C) {
	instance := s.manager.makeInstance(s.getInstruction(5))
	s.manager.commitInstance(instance, false)
	instance.getExecuteEvent()

	broadcast := false
	broadcastListener := func() {
		instance.getExecuteEvent().wait()
		broadcast = true
	}
	go broadcastListener()
	runtime.Gosched() // yield goroutine

	c.Check(broadcast, gocheck.Equals, false)
	c.Check(instance.executeEvent, gocheck.NotNil)

	_, err := s.manager.applyInstance(instance)
	runtime.Gosched() // yield goroutine
	c.Assert(err, gocheck.IsNil)

	c.Check(broadcast, gocheck.Equals, true)
}

// tests that apply instance marks the instance as
// executed, and moves it into the executed container
func (s *ExecuteApplyInstanceTest) TestBookKeeping(c *gocheck.C) {
	instance := s.manager.makeInstance(s.getInstruction(5))
	iid := instance.InstanceID
	s.manager.commitInstance(instance, false)

	// sanity check
	c.Check(s.manager.committed, instMapContainsKey, iid)
	c.Check(s.manager.executed, gocheck.Not(instIdSliceContains), iid)
	c.Check(instance.Status, gocheck.Equals, INSTANCE_COMMITTED)

	_, err := s.manager.applyInstance(instance)
	c.Assert(err, gocheck.IsNil)

	// check expected state
	c.Check(s.manager.committed, gocheck.Not(instMapContainsKey), iid)
	c.Check(s.manager.executed, instIdSliceContains, iid)
	c.Check(instance.Status, gocheck.Equals, INSTANCE_EXECUTED)
}

// tests that apply instance fails if the instance is not committed
func (s *ExecuteApplyInstanceTest) TestUncommittedFailure(c *gocheck.C) {
	instance := s.manager.makeInstance(s.getInstruction(5))
	iid := instance.InstanceID
	s.manager.acceptInstance(instance, false)

	// sanity check
	c.Check(s.manager.inProgress, instMapContainsKey, iid)
	c.Check(s.manager.executed, gocheck.Not(instIdSliceContains), iid)
	c.Check(instance.Status, gocheck.Equals, INSTANCE_ACCEPTED)

	_, err := s.manager.applyInstance(instance)
	c.Assert(err, gocheck.NotNil)
}
