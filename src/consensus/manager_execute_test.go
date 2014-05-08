/**
tests the execution of committed commands
*/
package consensus

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

import (
	"launchpad.net/gocheck"
)

import (
	"node"
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
	exOrder, uncommitted, err := s.manager.getExecutionOrder(s.toExecute)
	c.Assert(err, gocheck.IsNil)
	// commit all but the first instance
	for _, iid := range exOrder[1:] {
		instance := s.manager.instances.Get(iid)
		err := s.manager.commitInstance(instance, false)
		c.Assert(err, gocheck.IsNil)
	}
	exOrder, uncommitted, err = s.manager.getExecutionOrder(s.toExecute)
	c.Assert(err, gocheck.IsNil)
	c.Assert(len(uncommitted), gocheck.Equals, 1)
	s.toPrepare = s.manager.instances.Get(exOrder[0])
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
	listener := s.toExecute.addListener()
	err := s.manager.executeInstance(s.toExecute)
	c.Assert(err, gocheck.IsNil)


	result := <-listener
	c.Assert(result.err, gocheck.IsNil)
	c.Assert(result.val, gocheck.NotNil)
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
	listener := s.toExecute.addListener()

	err := s.manager.executeInstance(s.toExecute)
	c.Assert(err, gocheck.IsNil)

	result := <-listener
	c.Assert(result.err, gocheck.IsNil)
	c.Assert(result.val, gocheck.NotNil)
	c.Assert(s.preparePhaseCalls, gocheck.Equals, 2)

	c.Check(s.toPrepare.Status, gocheck.Equals, INSTANCE_EXECUTED)
	c.Check(s.toExecute.Status, gocheck.Equals, INSTANCE_EXECUTED)
}

// tests that an explicit prepare which is retried, will wait before
// retrying, but will abort if a commit notify event is broadcasted
func (s *ExecuteInstanceTest) TestExplicitPrepareRetryCondAbort(c *gocheck.C) {
	// skip waiting
	oldBallotFailureWaitTime := BALLOT_FAILURE_WAIT_TIME
	// FIXME: this test watis for the entire 10 seconds sometimes
	BALLOT_FAILURE_WAIT_TIME = uint64(10000)
	defer func() { BALLOT_FAILURE_WAIT_TIME = oldBallotFailureWaitTime }()

	s.patchPreparePhase(NewBallotError("nope"), false)

	s.toExecute.getExecuteEvent()
	var err error
	listener := s.toExecute.addListener()
	go func() { err = s.manager.executeInstance(s.toExecute) }()
	runtime.Gosched()

	// 'commit' and notify while prepare waits
	c.Assert(s.toPrepare.commitEvent, gocheck.NotNil)
	cerr := s.manager.commitInstance(s.toPrepare, false)
	c.Assert(cerr, gocheck.IsNil)
	s.toPrepare.broadcastCommitEvent()

	s.toExecute.getExecuteEvent().wait()

	result := <- listener
	c.Assert(err, gocheck.IsNil)
	c.Assert(result.val, gocheck.NotNil)
	c.Assert(result.err, gocheck.IsNil)

	c.Assert(s.preparePhaseCalls, gocheck.Equals, 1)

	c.Check(s.toPrepare.Status, gocheck.Equals, INSTANCE_EXECUTED)
	c.Check(s.toExecute.Status, gocheck.Equals, INSTANCE_EXECUTED)
}

// tests that execute will return an error if a prepare call returns
// a non ballot related error
func (s *ExecuteInstanceTest) TestExplicitPrepareFailure(c *gocheck.C) {
	s.patchPreparePhase(fmt.Errorf("negative"), false)
	listener := s.toExecute.addListener()
	err := s.manager.executeInstance(s.toExecute)
	c.Assert(err, gocheck.NotNil)

	c.Assert(len(listener), gocheck.Equals, 0)
	c.Assert(s.preparePhaseCalls, gocheck.Equals, 1)

	c.Check(s.toPrepare.Status, gocheck.Equals, INSTANCE_PREACCEPTED)
	c.Check(s.toExecute.Status, gocheck.Equals, INSTANCE_COMMITTED)
}

// tests that execute will return an error if it receives more ballot
// failures than the BALLOT_FAILURE_RETRIES value
func (s *ExecuteInstanceTest) TestExplicitPrepareBallotFailure(c *gocheck.C) {
	s.patchPreparePhase(NewBallotError("nope"), false)
	listener := s.toExecute.addListener()
	err := s.manager.executeInstance(s.toExecute)
	c.Assert(err, gocheck.NotNil)

	c.Assert(len(listener), gocheck.Equals, 0)
	c.Assert(s.preparePhaseCalls, gocheck.Equals, BALLOT_FAILURE_RETRIES)

	c.Check(s.toPrepare.Status, gocheck.Equals, INSTANCE_PREACCEPTED)
	c.Check(s.toExecute.Status, gocheck.Equals, INSTANCE_COMMITTED)
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
	actual, uncommitted, err := s.manager.getExecutionOrder(lastInstance)
	c.Assert(err, gocheck.IsNil)
	c.Assert(len(uncommitted), gocheck.Equals, 0)
	c.Assert(actual, gocheck.DeepEquals, s.expectedOrder)
}

// tests that the dependency ordering of connected instances is the same, regardless of the 'target' instance
func (s *ExecuteDependencyChainTest) TestInstanceDependentConnectedDependencyOrdering(c *gocheck.C) {
	s.commitInstances()

	inst := s.manager.makeInstance(s.getInstruction(6))
	s.manager.preAcceptInstanceUnsafe(inst, false)
	inst.commit(nil, false)
	c.Assert(len(inst.Dependencies), gocheck.Equals, len(s.expectedOrder))
	s.expectedOrder = append(s.expectedOrder, inst.InstanceID)

	for i, iid := range s.expectedOrder {
		instance := s.manager.instances.Get(iid)
		var expected []InstanceID
		if i > 5 {
			expected = s.expectedOrder
		} else if i > 2 {
			expected = s.expectedOrder[:6]
		} else {
			expected = s.expectedOrder[:3]
		}
		actual, uncommitted, err := s.manager.getExecutionOrder(instance)
		c.Assert(err, gocheck.IsNil)
		c.Assert(len(uncommitted), gocheck.Equals, 0)
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
		numExpectedDeps := i
		c.Assert(len(instance.Dependencies), gocheck.Equals, numExpectedDeps, gocheck.Commentf("instance: %v", i))
		s.manager.commitInstance(instance, false)
		instances[i] = instance
	}

	expected := make([]InstanceID, len(instances))
	for i, instance := range instances {
		expected[i] = instance.InstanceID
	}

	for i, instance := range instances {
		actual, uncommitted, err := s.manager.getExecutionOrder(instance)
		c.Assert(err, gocheck.IsNil)
		c.Assert(len(uncommitted), gocheck.Equals, 0)
		c.Assert(actual, gocheck.DeepEquals, expected[:i+1], gocheck.Commentf("instance %v", i))
	}
}

// getExecutionOrder should return an error if an instance
// depends on an instance that the manager hasn't seen
func (s *ExecuteDependencyChainTest) TestMissingDependencyOrder(c *gocheck.C) {
	s.commitInstances()

	lastInstance := s.manager.instances.Get(s.expectedOrder[len(s.expectedOrder) - 1])
	lastInstance.Dependencies = append(lastInstance.Dependencies, NewInstanceID())
	actual, uncommitted, err := s.manager.getExecutionOrder(lastInstance)
	c.Assert(actual, gocheck.IsNil)
	c.Assert(uncommitted, gocheck.IsNil)
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
	resultListener := targetInst.addListener()

	err := s.manager.executeInstance(targetInst)
	c.Assert(err, gocheck.IsNil)

	result := <-resultListener
	c.Assert(result.err, gocheck.IsNil)
	val := result.val
	c.Assert(val, gocheck.NotNil)

	// check stats
	c.Check(s.manager.stats.(*mockStatter).counters["execute.instance.success.count"], gocheck.Equals, int64(5))

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

// tests that getting the execution order saves the strongly connected component ids on the instances
func (s *ExecuteDependencyChainTest) TestInstanceStrongComponentsAreIncludedInExOrder(c *gocheck.C) {
	s.manager = NewManager(s.manager.cluster)
	var prevInstance *Instance
	instances := make([]*Instance, 0)
	component := make([]InstanceID, 0)
	for i:=0; i<3; i++ {
		instance := s.manager.makeInstance(getBasicInstruction())
		err := s.manager.commitInstance(instance, true)
		c.Assert(err, gocheck.IsNil)
		if prevInstance == nil {
			instance.Dependencies = []InstanceID{}
		} else {
			instance.Dependencies = []InstanceID{prevInstance.InstanceID}
		}
		instances = append(instances, instance)
		prevInstance = instance
		component = append(component, instance.InstanceID)
	}
	expectedSet := NewInstanceIDSet(component)

	// add the last instance as a dependency of the first instance
	instances[0].Dependencies = []InstanceID{instances[2].InstanceID}

	exOrder, uncommitted, err := s.manager.getExecutionOrder(instances[2])
	c.Assert(err, gocheck.IsNil)
	c.Assert(len(uncommitted), gocheck.Equals, 0)
	c.Check(exOrder, gocheck.DeepEquals, component)

	for i, instance := range instances {
		c.Check(instance.StronglyConnected, gocheck.DeepEquals, expectedSet, gocheck.Commentf("iteration %v", i))
	}
}

// tests that components of 1 are not recorded
func (s *ExecuteDependencyChainTest) TestSingleStrongComponentsAreSkipped(c *gocheck.C) {
	var err error
	s.manager = NewManager(s.manager.cluster)
	instance := s.manager.makeInstance(getBasicInstruction())
	err = s.manager.commitInstance(instance, true)
	c.Assert(err, gocheck.IsNil)

	_, _, err = s.manager.getExecutionOrder(instance)
	c.Assert(err, gocheck.IsNil)

	c.Check(instance.StronglyConnected.Size(), gocheck.Equals, 0)
}

// tests that, as large strongly connected components are executed, their execution ordering
// is not affected by the dependency graph excluding some executed instances
func (s *ExecuteDependencyChainTest) TestExecutingLongStronglyConnectedComponentOrdering(c *gocheck.C) {
	s.manager = NewManager(s.manager.cluster)
	var prevInstance *Instance
	instances := make([]*Instance, 0)
	for i:=0; i<10; i++ {
		instance := s.manager.makeInstance(getBasicInstruction())
		err := s.manager.commitInstance(instance, true)
		c.Assert(err, gocheck.IsNil)
		if prevInstance == nil {
			instance.Dependencies = []InstanceID{}
		} else {
			instance.Dependencies = []InstanceID{prevInstance.InstanceID}
		}
		instances = append(instances, instance)
		prevInstance = instance
	}

	// add the last instance as a dependency of the first instance
	instances[0].Dependencies = []InstanceID{instances[9].InstanceID}

	expected := make([]InstanceID, len(instances))
	for i, instance := range instances {
		expected[i] = instance.InstanceID
	}

	actual, uncommitted, err := s.manager.getExecutionOrder(instances[9])
	c.Assert(err, gocheck.IsNil)
	c.Assert(len(uncommitted), gocheck.Equals, 0)

	c.Check(actual, gocheck.DeepEquals, expected)

	for i, toExecute := range instances {
		toExecute.Status = INSTANCE_EXECUTED

		for j, instance := range instances[i:] {
			actual, uncommitted, err := s.manager.getExecutionOrder(instance)
			c.Assert(err, gocheck.IsNil)
			c.Assert(len(uncommitted), gocheck.Equals, 0)

			c.Check(actual, gocheck.DeepEquals, expected, gocheck.Commentf("Iteration: %v:%v", i, j))
		}
	}
}

func (s *ExecuteDependencyChainTest) TestRecordStronglyConnectedComponentsSuccess(c *gocheck.C) {
	s.manager = NewManager(s.manager.cluster)
	var prevInstance *Instance
	instances := make([]*Instance, 0)
	depMap := make(map[InstanceID]*Instance)
	component := make([]InstanceID, 0)
	for i:=0; i<10; i++ {
		instance := s.manager.makeInstance(getBasicInstruction())
		err := s.manager.commitInstance(instance, true)
		c.Assert(err, gocheck.IsNil)
		if prevInstance == nil {
			instance.Dependencies = []InstanceID{}
		} else {
			instance.Dependencies = []InstanceID{prevInstance.InstanceID}
		}
		instances = append(instances, instance)
		prevInstance = instance

		depMap[instance.InstanceID] = instance
		component = append(component, instance.InstanceID)
	}
	expectedSet := NewInstanceIDSet(component)

	// add the last instance as a dependency of the first instance
	instances[0].Dependencies = []InstanceID{instances[9].InstanceID}

	_, _, err := s.manager.getExecutionOrder(instances[9])
//	err := s.manager.recordStronglyConnectedComponents(component, depMap)
	c.Assert(err, gocheck.IsNil)

	for _, instance := range instances {
		c.Check(instance.StronglyConnected, gocheck.DeepEquals, expectedSet)
	}

}

func (s *ExecuteDependencyChainTest) TestRecordStronglyConnectedComponentsUncommittedComponent(c *gocheck.C) {
	s.manager = NewManager(s.manager.cluster)
	var prevInstance *Instance
	instances := make([]*Instance, 0)
	depMap := make(map[InstanceID]*Instance)
	component := make([]InstanceID, 0)
	for i:=0; i<3; i++ {
		instance := s.manager.makeInstance(getBasicInstruction())
		if i > 0 {
			err := s.manager.commitInstance(instance, true)
			c.Assert(err, gocheck.IsNil)
		} else {
			err := s.manager.preAcceptInstance(instance, true)
			c.Assert(err, gocheck.IsNil)
		}
		component = append(component, instance.InstanceID)
		if prevInstance == nil {
			instance.Dependencies = []InstanceID{}
		} else {
			instance.Dependencies = []InstanceID{prevInstance.InstanceID}
		}
		instances = append(instances, instance)
		prevInstance = instance

		depMap[instance.InstanceID] = instance
	}

	// connect instance 2 to instance 4
	instances[0].Dependencies = append(instances[0].Dependencies, instances[2].InstanceID)

	_, uncommitted, err := s.manager.getExecutionOrder(instances[2])
	c.Assert(err, gocheck.IsNil)
	c.Assert(len(uncommitted), gocheck.Equals, 1)

	// tests that none of the instance have any strongly connected components set
	for i, instance := range instances {
		c.Check(instance.StronglyConnected.Size(), gocheck.Equals, 0, gocheck.Commentf("iteration %v", i))
	}
}

// tests that strongly connected components are not recorded if the component has an uncommitted dep
func (s *ExecuteDependencyChainTest) TestRecordStronglyConnectedComponentsUncommittedDep(c *gocheck.C) {
	s.manager = NewManager(s.manager.cluster)
	var prevInstance *Instance
	instances := make([]*Instance, 0)
	depMap := make(map[InstanceID]*Instance)
	component := make([]InstanceID, 0)
	for i:=0; i<5; i++ {
		instance := s.manager.makeInstance(getBasicInstruction())
		if i > 0 {
			err := s.manager.commitInstance(instance, true)
			c.Assert(err, gocheck.IsNil)
		} else {
			err := s.manager.preAcceptInstance(instance, true)
			c.Assert(err, gocheck.IsNil)
		}
		if i > 1 {
			component = append(component, instance.InstanceID)
		}
		if prevInstance == nil {
			instance.Dependencies = []InstanceID{}
		} else {
			instance.Dependencies = []InstanceID{prevInstance.InstanceID}
		}
		instances = append(instances, instance)
		prevInstance = instance

		depMap[instance.InstanceID] = instance
		c.Assert(instance.StronglyConnected.Size(), gocheck.Equals, 0)
	}

	// connect instance 2 to instance 4
	instances[2].Dependencies = append(instances[2].Dependencies, instances[4].InstanceID)

	_, uncommitted, err := s.manager.getExecutionOrder(instances[4])
	c.Assert(err, gocheck.IsNil)
	c.Assert(len(uncommitted), gocheck.Equals, 1)

	// tests that none of the instance have any strongly connected components set
	for i, instance := range instances {
		c.Check(instance.StronglyConnected.Size(), gocheck.Equals, 0, gocheck.Commentf("iteration %v", i))
	}
}

// tests that noop instances are not applied to the store
func (s *ExecuteDependencyChainTest) TestRejectedInstanceSkip(c *gocheck.C) {
	s.commitInstances()
	rejectInst := s.manager.instances.Get(s.expectedOrder[0])
	rejectInst.Noop = true
	targetInst := s.manager.instances.Get(s.expectedOrder[1])
	resultListener := targetInst.addListener()


	err := s.manager.executeInstance(targetInst)

	result := <-resultListener

	c.Assert(err, gocheck.IsNil)
	c.Assert(result.err, gocheck.IsNil)
	c.Assert(result.val, gocheck.NotNil)
	c.Check(result.val.(*intVal).value, gocheck.Equals, 1)

	// check the number of instructions
	c.Assert(len(s.cluster.instructions), gocheck.Equals, 1)
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
	resultListener := targetInst.addListener()

	err := s.manager.executeInstance(targetInst)
	c.Assert(err, gocheck.IsNil)

	result := <-resultListener
	c.Assert(result.err, gocheck.IsNil)
	c.Assert(result.val, gocheck.NotNil)

	// the target instance should have been executed
	c.Check(targetInst.Status, gocheck.Equals, INSTANCE_EXECUTED)

	// and the cluster should have received only one instruction
	c.Assert(len(s.cluster.instructions), gocheck.Equals, 1)
	c.Check(s.cluster.instructions[0].Args[0], gocheck.Equals, fmt.Sprint(s.maxIdx))
}

// tests that an error is returned if an uncommitted instance id is provided
func (s *ExecuteDependencyChainTest) TestUncommittedFailure(c *gocheck.C) {
	targetInst := s.manager.instances.Get(s.expectedOrder[0])

	err := s.manager.executeDependencyChain(s.expectedOrder, targetInst)

	c.Assert(err, gocheck.NotNil)
}

// tests that an error is returned if a dependency is not committed
func (s *ExecuteDependencyChainTest) TestUncommittedDependencyFailure(c *gocheck.C) {
	uncommittedInst := s.manager.instances.Get(s.expectedOrder[0])
	uncommittedInst.Status = INSTANCE_PREACCEPTED
	targetInst := s.manager.instances.Get(s.expectedOrder[5])

	err := s.manager.executeDependencyChain(s.expectedOrder, targetInst)

	c.Assert(err, gocheck.NotNil)
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

// tests that noop instances aren't applied to the store
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

// tests that goroutines listening on an instance are
// notified of the result when it executes
func (s *ExecuteApplyInstanceTest) TestResultListenerBroadcast(c *gocheck.C) {
	instance := s.manager.makeInstance(s.getInstruction(5))
	instance.Status = INSTANCE_COMMITTED

	var result InstanceResult
	resultListener := instance.addListener()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func(){
		result = <- resultListener
		wg.Done()
	}()
	val, err := s.manager.applyInstance(instance)
	wg.Wait()

	c.Assert(err, gocheck.IsNil)
	c.Assert(val, gocheck.NotNil)
	c.Assert(result.err, gocheck.IsNil)
	c.Assert(result.val, gocheck.Equals, val)
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
	c.Check(s.manager.executed, gocheck.Not(instIdSliceContains), iid)
	c.Check(instance.Status, gocheck.Equals, INSTANCE_COMMITTED)

	_, err := s.manager.applyInstance(instance)
	c.Assert(err, gocheck.IsNil)

	// check expected state
	c.Check(s.manager.executed, instIdSliceContains, iid)
	c.Check(instance.Status, gocheck.Equals, INSTANCE_EXECUTED)
}

// tests that apply instance fails if the instance is not committed
func (s *ExecuteApplyInstanceTest) TestUncommittedFailure(c *gocheck.C) {
	instance := s.manager.makeInstance(s.getInstruction(5))
	iid := instance.InstanceID
	s.manager.acceptInstance(instance, false)

	// sanity check
	c.Check(s.manager.executed, gocheck.Not(instIdSliceContains), iid)
	c.Check(instance.Status, gocheck.Equals, INSTANCE_ACCEPTED)

	_, err := s.manager.applyInstance(instance)
	c.Assert(err, gocheck.NotNil)
}

// tests that instances are marked as executed
func (s *ExecuteApplyInstanceTest) TestReportExecuted(c *gocheck.C) {
	var err error
	instance := s.manager.makeInstance(s.getInstruction(5))

	// check that this instance hasn't already been somehow acknowledged
	depsNode := s.manager.depsMngr.deps.get("a")
	c.Assert(depsNode.executed.Contains(instance.InstanceID), gocheck.Equals, false)

	err = s.manager.commitInstance(instance, false)
	c.Assert(err, gocheck.IsNil)
	_, err = s.manager.applyInstance(instance)
	c.Assert(err, gocheck.IsNil)

	// check that the dependency manager knows this instance was executed
	c.Assert(depsNode.executed.Contains(instance.InstanceID), gocheck.Equals, true)
}
