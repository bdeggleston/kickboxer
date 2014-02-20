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
	baseScopeTest
	expectedOrder []InstanceID
	maxIdx int
}

// makes a set of interdependent instances, and sets
// their expected ordering
func (s *baseExecutionTest) SetUpTest(c *gocheck.C) {
	s.baseScopeTest.SetUpTest(c)
	s.expectedOrder  = make([]InstanceID, 0)
	lastVal := 0

	// sets up a new instance, and appends it to the expected order needs
	// to be called in the same order as the expected dependency ordering
	addInst := func() *Instance {
		inst := s.scope.makeInstance(s.getInstructions(lastVal))
		inst.Successors = []node.NodeId{inst.LeaderID}
		lastVal++
		s.scope.preAcceptInstanceUnsafe(inst, false)
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
	oldPreparePhase func(*Scope, *Instance) error
	oldBallotTimeout uint64
	preparePhaseCalls int

	toPrepare *Instance
	toExecute *Instance
}

var _ = gocheck.Suite(&ExecuteInstanceTest{})

func (s *ExecuteInstanceTest) SetUpSuite(c *gocheck.C) {
	s.oldBallotTimeout = BALLOT_FAILURE_WAIT_TIME
	BALLOT_FAILURE_WAIT_TIME = uint64(5)
}

func (s *ExecuteInstanceTest) TearDownSuite(c *gocheck.C) {
	BALLOT_FAILURE_WAIT_TIME = s.oldBallotTimeout
}

func (s *ExecuteInstanceTest) SetUpTest(c *gocheck.C) {
	s.baseExecutionTest.SetUpTest(c)
	s.oldPreparePhase = scopePreparePhase
	s.preparePhaseCalls = 0

	// make all instances 'timed out'
	for _, iid := range s.expectedOrder {
		instance := s.scope.instances.Get(iid)
		instance.commitTimeout = time.Now().Add(time.Duration(-1) * time.Millisecond)
	}
	s.toExecute = s.scope.instances.Get(s.expectedOrder[2])
	exOrder, err := s.scope.getExecutionOrder(s.toExecute)
	c.Assert(err, gocheck.IsNil)
	// commit all but the first instance
	for _, iid := range exOrder[1:] {
		instance := s.scope.instances.Get(iid)
		err := s.scope.commitInstance(instance, false)
		c.Assert(err, gocheck.IsNil)
	}
	s.toPrepare = s.scope.instances.Get(exOrder[0])
	c.Assert(len(s.scope.getUncommittedInstances(exOrder)), gocheck.Equals, 1)
}

func (s *ExecuteInstanceTest) TearDownTest(c *gocheck.C) {
	scopePreparePhase = s.oldPreparePhase
}

func (s *ExecuteInstanceTest) patchPreparePhase(err error, commit bool) {
	scopePreparePhase = func(scope *Scope, instance *Instance) error {
		s.preparePhaseCalls++
		if commit {
			scope.commitInstance(instance, false)
		}
		return err
	}
}

// tests that an explicit prepare is initiated on any uncommitted
// instance in the instance's dependency graph
func (s *ExecuteInstanceTest) TestExplicitPrepare(c *gocheck.C) {
	s.patchPreparePhase(nil, true)
	val, err := s.scope.executeInstance(s.toExecute)
	c.Assert(val, gocheck.NotNil)
	c.Assert(err, gocheck.IsNil)
	c.Assert(s.preparePhaseCalls, gocheck.Equals, 1)

	c.Check(s.toPrepare.Status, gocheck.Equals, INSTANCE_EXECUTED)
	c.Check(s.toExecute.Status, gocheck.Equals, INSTANCE_EXECUTED)

}

// tests that an explicit prepare is retried if it fails due to
// a ballot failure
func (s *ExecuteInstanceTest) TestExplicitPrepareRetry(c *gocheck.C) {
	oldBallotFailureWaitTime := BALLOT_FAILURE_WAIT_TIME
	defer func() {BALLOT_FAILURE_WAIT_TIME = oldBallotFailureWaitTime}()
	BALLOT_FAILURE_WAIT_TIME = uint64(5)

	// die once, succeed on second try
	scopePreparePhase = func(scope *Scope, instance *Instance) error {
		if s.preparePhaseCalls > 0 {
			scope.commitInstance(instance, false)
		} else {
			s.preparePhaseCalls++
			return NewBallotError("nope")
		}
		s.preparePhaseCalls++
		return nil
	}

	val, err := s.scope.executeInstance(s.toExecute)
	c.Assert(val, gocheck.NotNil)
	c.Assert(err, gocheck.IsNil)
	c.Assert(s.preparePhaseCalls, gocheck.Equals, 2)

	c.Check(s.toPrepare.Status, gocheck.Equals, INSTANCE_EXECUTED)
	c.Check(s.toExecute.Status, gocheck.Equals, INSTANCE_EXECUTED)
}

// tests that an explicit prepare which is retried, will wait before
// retrying, but will abort if a commit notify event is broadcasted
func (s *ExecuteInstanceTest) TestExplicitPrepareRetryCondAbort(c *gocheck.C) {
	// TODO: I think this test is actually waiting 10 seconds
	oldBallotFailureWaitTime := BALLOT_FAILURE_WAIT_TIME
	BALLOT_FAILURE_WAIT_TIME = uint64(10000)
	defer func() { BALLOT_FAILURE_WAIT_TIME = oldBallotFailureWaitTime }()
	s.patchPreparePhase(NewBallotError("nope"), false)

	s.toExecute.getExecuteEvent()
	var val store.Value
	var err error
	go func() { val, err = s.scope.executeInstance(s.toExecute) }()
	runtime.Gosched()

	// 'commit' and notify while prepare waits
	c.Assert(s.toPrepare.commitEvent, gocheck.NotNil)
	cerr := s.scope.commitInstance(s.toPrepare, false)
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
	val, err := s.scope.executeInstance(s.toExecute)
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
	val, err := s.scope.executeInstance(s.toExecute)
	c.Assert(val, gocheck.IsNil)
	c.Assert(err, gocheck.NotNil)
	c.Assert(s.preparePhaseCalls, gocheck.Equals, BALLOT_FAILURE_RETRIES)

	c.Check(s.toPrepare.Status, gocheck.Equals, INSTANCE_PREACCEPTED)
	c.Check(s.toExecute.Status, gocheck.Equals, INSTANCE_COMMITTED)
}

// tests that if a prepare phase removes an instance from the target
// instance's dependency graph, it's not executed
func (s *ExecuteInstanceTest) TestPrepareExOrderChange(c *gocheck.C) {
	scopePreparePhase = func(scope *Scope, instance *Instance) error {
		instance.Sequence += 5
		s.preparePhaseCalls++
		scope.commitInstance(instance, false)
		return nil
	}

	val, err := s.scope.executeInstance(s.toExecute)
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
		s.scope.commitInstance(s.scope.instances.Get(iid), false)
	}
}

func (s *ExecuteDependencyChainTest) TestDependencyOrdering(c *gocheck.C) {
	s.commitInstances()

	lastInstance := s.scope.instances.Get(s.expectedOrder[len(s.expectedOrder) - 1])
	actual, err := s.scope.getExecutionOrder(lastInstance)
	c.Assert(err, gocheck.IsNil)
	c.Assert(len(s.expectedOrder), gocheck.Equals, len(actual))
	for i := range s.expectedOrder {
		c.Check(s.expectedOrder[i], gocheck.Equals, actual[i], gocheck.Commentf("iid %v", i))
	}
}

// getExecutionOrder should return an error if an instance
// depends on an instance that the scope hasn't seen
func (s *ExecuteDependencyChainTest) TestMissingDependencyOrder(c *gocheck.C) {
	s.commitInstances()

	lastInstance := s.scope.instances.Get(s.expectedOrder[len(s.expectedOrder) - 1])
	lastInstance.Dependencies = append(lastInstance.Dependencies, NewInstanceID())
	actual, err := s.scope.getExecutionOrder(lastInstance)
	c.Assert(actual, gocheck.IsNil)
	c.Assert(err, gocheck.NotNil)
}

// tests that instances up to and including the given
// instance are executed when the dependencies all
// have a remote instance id
func (s *ExecuteDependencyChainTest) TestExternalDependencySuccess(c *gocheck.C) {
	s.commitInstances()
	targetInst := s.scope.instances.Get(s.expectedOrder[s.maxIdx - 1])

	// set non-target dependency leaders to a remote node
	remoteID := node.NewNodeId()
	for i, iid := range s.expectedOrder {
		if i > (s.maxIdx - 2) { break }
		instance := s.scope.instances.Get(iid)
		instance.LeaderID = remoteID
	}

	val, err := s.scope.executeInstance(targetInst)

	c.Assert(err, gocheck.IsNil)
	c.Assert(val, gocheck.NotNil)

	// check stats
	c.Check(s.scope.manager.stats.(*mockStatter).counters["execute.remote.success.count"], gocheck.Equals, int64(4))
	c.Check(s.scope.manager.stats.(*mockStatter).counters["execute.local.success.count"], gocheck.Equals, int64(1))
	c.Check(s.scope.manager.stats.(*mockStatter).counters["execute.instance.apply.count"], gocheck.Equals, int64(5))

	// check returned value
	c.Assert(&intVal{}, gocheck.FitsTypeOf, val)
	c.Check(4, gocheck.Equals, val.(*intVal).value)

	// check the number of instructions
	c.Assert(len(s.cluster.instructions), gocheck.Equals, len(s.expectedOrder) - 1)

	// check all the instances, instructions, etc
	for i:=0; i<len(s.expectedOrder); i++ {
		instance := s.scope.instances.Get(s.expectedOrder[i])

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
	rejectInst := s.scope.instances.Get(s.expectedOrder[0])
	rejectInst.Noop = true
	targetInst := s.scope.instances.Get(s.expectedOrder[1])

	val, err := s.scope.executeInstance(targetInst)

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
	targetInst := s.scope.instances.Get(s.expectedOrder[len(s.expectedOrder) - 2])

	for i, iid := range s.expectedOrder {
		if i > (s.maxIdx - 2) { break }
		instance := s.scope.instances.Get(iid)
		instance.executeTimeout = time.Now().Add(time.Duration(-1) * time.Second)
	}

	val, err := s.scope.executeInstance(targetInst)

	c.Assert(err, gocheck.IsNil)
	c.Assert(val, gocheck.NotNil)

	// check stats
	stats := s.scope.manager.stats.(*mockStatter)
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
		instance := s.scope.instances.Get(s.expectedOrder[i])

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
	depInst := s.scope.instances.Get(s.expectedOrder[0])
	depInst.executeTimeout = time.Now().Add(time.Duration(10) * time.Millisecond)
	targetInst := s.scope.instances.Get(s.expectedOrder[1])

	val, err := s.scope.executeInstance(targetInst)

	c.Assert(err, gocheck.IsNil)
	c.Assert(val, gocheck.NotNil)

	// check stats
	stats := s.scope.manager.stats.(*mockStatter)
	c.Check(stats.counters["execute.local.timeout.count"], gocheck.Equals, int64(1))
	c.Check(stats.counters["execute.local.timeout.wait.count"], gocheck.Equals, int64(1))
	c.Check(stats.counters["execute.local.success.count"], gocheck.Equals, int64(1))
	c.Check(stats.counters["execute.instance.apply.count"], gocheck.Equals, int64(2))

	// check the number of instructions
	c.Assert(len(s.cluster.instructions), gocheck.Equals, 2)

	// check all the instances, instructions, etc
	for i:=0; i<2; i++ {
		instance := s.scope.instances.Get(s.expectedOrder[i])

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
	depInst := s.scope.instances.Get(s.expectedOrder[0])
	depInst.executeTimeout = time.Now().Add(time.Duration(1) * time.Minute)
	targetInst := s.scope.instances.Get(s.expectedOrder[1])

	var val store.Value
	var err error

	depInst.getExecuteEvent()

	go func() { val, err = s.scope.executeInstance(targetInst) }()
	runtime.Gosched()  // yield

	// goroutine should be waiting
	c.Check(s.scope.manager.stats.(*mockStatter).counters["execute.instance.apply.count"], gocheck.Equals, int64(0))

	// release wait
	depInst.broadcastExecuteEvent()
	runtime.Gosched()

	c.Assert(err, gocheck.IsNil)
	c.Assert(val, gocheck.NotNil)

	// check stats
	stats := s.scope.manager.stats.(*mockStatter)
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
	targetInst := s.scope.instances.Get(s.expectedOrder[s.maxIdx])

	// execute all but the target dependency
	for i:=0; i<s.maxIdx; i++ {
		instance := s.scope.instances.Get(s.expectedOrder[i])
		instance.Status = INSTANCE_EXECUTED
	}

	val, err := s.scope.executeInstance(targetInst)

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
	targetInst := s.scope.instances.Get(s.expectedOrder[0])

	val, err := s.scope.executeDependencyChain(s.expectedOrder, targetInst)

	c.Assert(err, gocheck.NotNil)
	c.Assert(val, gocheck.IsNil)
}

type ExecuteApplyInstanceTest struct {
	baseScopeTest
}

var _ = gocheck.Suite(&ExecuteApplyInstanceTest{})

func (s *ExecuteApplyInstanceTest) TestSuccess(c *gocheck.C) {
	instance := s.scope.makeInstance(s.getInstructions(5))
	err := s.scope.commitInstance(instance, false)
	c.Assert(err, gocheck.IsNil)
	val, err := s.scope.applyInstance(instance)
	c.Assert(err, gocheck.IsNil)
	c.Assert(val, gocheck.FitsTypeOf, &intVal{})
	c.Assert(val.(*intVal).value, gocheck.Equals, 5)
	c.Check(instance.Status, gocheck.Equals, INSTANCE_EXECUTED)
	c.Check(len(s.cluster.instructions), gocheck.Equals, 1)
	c.Check(s.cluster.values["a"].value, gocheck.Equals, 5)
}

//
func (s *ExecuteApplyInstanceTest) TestSkipRejectedInstance(c *gocheck.C) {
	instance := s.scope.makeInstance(s.getInstructions(5))
	instance.Status = INSTANCE_COMMITTED
	instance.Noop = true
	val, err := s.scope.applyInstance(instance)
	c.Assert(err, gocheck.IsNil)
	c.Assert(val, gocheck.IsNil)
	c.Check(instance.Noop, gocheck.Equals, true)
	c.Check(len(s.cluster.instructions), gocheck.Equals, 0)
}

// tests the executing an instance against the store
// broadcasts to an existing notify instance, and
// removes it from the executeNotify map
func (s *ExecuteApplyInstanceTest) TestNotifyHandling(c *gocheck.C) {
	instance := s.scope.makeInstance(s.getInstructions(5))
	s.scope.commitInstance(instance, false)
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

	_, err := s.scope.applyInstance(instance)
	runtime.Gosched() // yield goroutine
	c.Assert(err, gocheck.IsNil)

	c.Check(broadcast, gocheck.Equals, true)
}

// tests that apply instance marks the instance as
// executed, and moves it into the executed container
func (s *ExecuteApplyInstanceTest) TestBookKeeping(c *gocheck.C) {
	instance := s.scope.makeInstance(s.getInstructions(5))
	iid := instance.InstanceID
	s.scope.commitInstance(instance, false)

	// sanity check
	c.Check(s.scope.committed, instMapContainsKey, iid)
	c.Check(s.scope.executed, gocheck.Not(instIdSliceContains), iid)
	c.Check(instance.Status, gocheck.Equals, INSTANCE_COMMITTED)

	_, err := s.scope.applyInstance(instance)
	c.Assert(err, gocheck.IsNil)

	// check expected state
	c.Check(s.scope.committed, gocheck.Not(instMapContainsKey), iid)
	c.Check(s.scope.executed, instIdSliceContains, iid)
	c.Check(instance.Status, gocheck.Equals, INSTANCE_EXECUTED)
}

// tests that apply instance fails if the instance is not committed
func (s *ExecuteApplyInstanceTest) TestUncommittedFailure(c *gocheck.C) {
	instance := s.scope.makeInstance(s.getInstructions(5))
	iid := instance.InstanceID
	s.scope.acceptInstance(instance, false)

	// sanity check
	c.Check(s.scope.inProgress, instMapContainsKey, iid)
	c.Check(s.scope.executed, gocheck.Not(instIdSliceContains), iid)
	c.Check(instance.Status, gocheck.Equals, INSTANCE_ACCEPTED)

	_, err := s.scope.applyInstance(instance)
	c.Assert(err, gocheck.NotNil)
}
