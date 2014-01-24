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

type ExecuteInstanceTest struct {
	baseScopeTest
}

var _ = gocheck.Suite(&ExecuteInstanceTest{})

// tests that an explicit prepare is initiated on any uncommitted
// instance in the instance's dependency graph
func (s *ExecuteInstanceTest) TestExplicitPrepare(c *gocheck.C) {

}

// tests that an explicit prepare is retried if it fails due to
// a ballot failure
func (s *ExecuteInstanceTest) TestExplicitPrepareRetry(c *gocheck.C) {

}

// tests that an explicit prepare which is retried, will wait before
// retrying, but will abort if a commit notify event is broadcasted
func (s *ExecuteInstanceTest) TestExplicitPrepareRetryCondAbort(c *gocheck.C) {

}

type ExecuteDependencyChainTest struct {
	baseScopeTest
	expectedOrder []InstanceID
	maxIdx int
}

var _ = gocheck.Suite(&ExecuteDependencyChainTest{})

// makes a set of interdependent instances, and sets
// their expected ordering
func (s *ExecuteDependencyChainTest) SetUpTest(c *gocheck.C) {
	s.baseScopeTest.SetUpTest(c)
	s.expectedOrder  = make([]InstanceID, 0)
	lastVal := 0

	// sets up a new instance, and appends it to the expected order needs
	// to be called in the same order as the expected dependency ordering
	addInst := func() *Instance {
		inst := s.scope.makeInstance(s.getInstructions(lastVal))
		lastVal++
		s.scope.preAcceptInstanceUnsafe(inst)
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

// commits all instances
func (s *ExecuteDependencyChainTest) commitInstances() {
	for _, iid := range s.expectedOrder {
		s.scope.commitInstance(s.scope.instances[iid])
	}
}

func (s *ExecuteDependencyChainTest) TestDependencyOrdering(c *gocheck.C) {
	s.commitInstances()

	lastInstance := s.scope.instances[s.expectedOrder[len(s.expectedOrder) - 1]]
	actual := s.scope.getExecutionOrder(lastInstance)
	c.Assert(len(s.expectedOrder), gocheck.Equals, len(actual))
	for i := range s.expectedOrder {
		c.Check(s.expectedOrder[i], gocheck.Equals, actual[i], gocheck.Commentf("iid %v", i))
	}
}

// tests that instances up to and including the given
// instance are executed when the dependencies all
// have a remote instance id
func (s *ExecuteDependencyChainTest) TestExternalDependencySuccess(c *gocheck.C) {
	s.commitInstances()
	targetInst := s.scope.instances[s.expectedOrder[s.maxIdx - 1]]

	// set non-target dependency leaders to a remote node
	remoteID := node.NewNodeId()
	for i, iid := range s.expectedOrder {
		if i > (s.maxIdx - 2) { break }
		instance := s.scope.instances[iid]
		instance.LeaderID = remoteID
	}

	val, err := s.scope.executeInstance(targetInst)

	c.Assert(err, gocheck.IsNil)
	c.Assert(val, gocheck.NotNil)

	// check stats
	c.Check(int(s.scope.statExecuteRemote), gocheck.Equals, 4)
	c.Check(int(s.scope.statExecuteLocalSuccess), gocheck.Equals, 1)
	c.Check(int(s.scope.statExecuteCount), gocheck.Equals, 5)

	// check returned value
	c.Assert(&intVal{}, gocheck.FitsTypeOf, val)
	c.Check(4, gocheck.Equals, val.(*intVal).value)

	// check the number of instructions
	c.Assert(len(s.cluster.instructions), gocheck.Equals, len(s.expectedOrder) - 1)

	// check all the instances, instructions, etc
	for i:=0; i<len(s.expectedOrder); i++ {
		instance := s.scope.instances[s.expectedOrder[i]]

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

// tests the execution of dependency chains when all of the target dependencies
// have are past their execution grace period
func (s *ExecuteDependencyChainTest) TestTimedOutLocalDependencySuccess(c *gocheck.C) {
	s.commitInstances()
	targetInst := s.scope.instances[s.expectedOrder[len(s.expectedOrder) - 2]]

	for i, iid := range s.expectedOrder {
		if i > (s.maxIdx - 2) { break }
		instance := s.scope.instances[iid]
		instance.executeTimeout = time.Now().Add(time.Duration(-1) * time.Second)
	}

	val, err := s.scope.executeInstance(targetInst)

	c.Assert(err, gocheck.IsNil)
	c.Assert(val, gocheck.NotNil)

	// check stats
	c.Check(int(s.scope.statExecuteLocalTimeout), gocheck.Equals, 4)
	c.Check(int(s.scope.statExecuteLocalTimeoutWait), gocheck.Equals, 0)
	c.Check(int(s.scope.statExecuteLocalSuccess), gocheck.Equals, 1)
	c.Check(int(s.scope.statExecuteCount), gocheck.Equals, 5)

	// check returned value
	c.Assert(&intVal{}, gocheck.FitsTypeOf, val)
	c.Check(4, gocheck.Equals, val.(*intVal).value)

	// check the number of instructions
	c.Assert(len(s.cluster.instructions), gocheck.Equals, len(s.expectedOrder) - 1)

	// check all the instances, instructions, etc
	for i:=0; i<len(s.expectedOrder); i++ {
		instance := s.scope.instances[s.expectedOrder[i]]

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
	depInst := s.scope.instances[s.expectedOrder[0]]
	depInst.executeTimeout = time.Now().Add(time.Duration(10) * time.Millisecond)
	targetInst := s.scope.instances[s.expectedOrder[1]]

	val, err := s.scope.executeInstance(targetInst)

	c.Assert(err, gocheck.IsNil)
	c.Assert(val, gocheck.NotNil)

	// check stats
	c.Check(int(s.scope.statExecuteLocalTimeout), gocheck.Equals, 1)
	c.Check(int(s.scope.statExecuteLocalTimeoutWait), gocheck.Equals, 1)
	c.Check(int(s.scope.statExecuteLocalSuccess), gocheck.Equals, 1)
	c.Check(int(s.scope.statExecuteCount), gocheck.Equals, 2)

	// check the number of instructions
	c.Assert(len(s.cluster.instructions), gocheck.Equals, 2)

	// check all the instances, instructions, etc
	for i:=0; i<2; i++ {
		instance := s.scope.instances[s.expectedOrder[i]]

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
	depInst := s.scope.instances[s.expectedOrder[0]]
	depInst.executeTimeout = time.Now().Add(time.Duration(1) * time.Minute)
	targetInst := s.scope.instances[s.expectedOrder[1]]

	var val store.Value
	var err error

	depNotify := makeConditional()
	s.scope.executeNotify[depInst.InstanceID] = depNotify

	go func() { val, err = s.scope.executeInstance(targetInst) }()
	runtime.Gosched()  // yield

	// goroutine should be waiting
	c.Check(int(s.scope.statExecuteCount), gocheck.Equals, 0)

	// release wait
	depNotify.Broadcast()
	runtime.Gosched()

	c.Assert(err, gocheck.IsNil)
	c.Assert(val, gocheck.NotNil)

	// check stats
	c.Check(int(s.scope.statExecuteLocalSuccessWait), gocheck.Equals, 1)
	c.Check(int(s.scope.statExecuteLocalTimeout), gocheck.Equals, 0)
	c.Check(int(s.scope.statExecuteLocalTimeoutWait), gocheck.Equals, 0)
	c.Check(int(s.scope.statExecuteLocalSuccess), gocheck.Equals, 1)
	c.Check(int(s.scope.statExecuteCount), gocheck.Equals, 1)

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
	targetInst := s.scope.instances[s.expectedOrder[s.maxIdx]]

	// execute all but the target dependency
	for i:=0; i<s.maxIdx; i++ {
		instance := s.scope.instances[s.expectedOrder[i]]
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
	targetInst := s.scope.instances[s.expectedOrder[0]]

	val, err := s.scope.executeDependencyChain(s.expectedOrder, targetInst)

	c.Assert(err, gocheck.NotNil)
	c.Assert(val, gocheck.IsNil)
}

type ApplyInstanceTest struct {
	baseScopeTest
}

var _ = gocheck.Suite(&ApplyInstanceTest{})

func (s *ApplyInstanceTest) TestSuccess(c *gocheck.C) {
	instance := s.scope.makeInstance(s.getInstructions(5))
	err := s.scope.commitInstance(instance)
	c.Assert(err, gocheck.IsNil)
	val, err := s.scope.applyInstance(instance)
	c.Assert(err, gocheck.IsNil)
	c.Assert(val, gocheck.FitsTypeOf, &intVal{})
	c.Assert(val.(*intVal).value, gocheck.Equals, 5)
	c.Check(instance.Status, gocheck.Equals, INSTANCE_EXECUTED)
	c.Check(len(s.cluster.instructions), gocheck.Equals, 1)
	c.Check(s.cluster.values["a"].value, gocheck.Equals, 5)
}

// tests the executing an instance against the store
// broadcasts to an existing notify instance, and
// removes it from the executeNotify map
func (s *ApplyInstanceTest) TestNotifyHandling(c *gocheck.C) {
	instance := s.scope.makeInstance(s.getInstructions(5))
	s.scope.commitInstance(instance)
	s.scope.executeNotify[instance.InstanceID] = makeConditional()

	broadcast := false
	broadcastListener := func() {
		cond := s.scope.executeNotify[instance.InstanceID]
		c.Check(cond, gocheck.NotNil)
		cond.Wait()
		broadcast = true
	}
	go broadcastListener()
	runtime.Gosched() // yield goroutine

	c.Check(broadcast, gocheck.Equals, false)
	c.Check(s.scope.executeNotify[instance.InstanceID], gocheck.NotNil)

	_, err := s.scope.applyInstance(instance)
	runtime.Gosched() // yield goroutine
	c.Assert(err, gocheck.IsNil)

	c.Check(broadcast, gocheck.Equals, true)
	c.Check(s.scope.executeNotify[instance.InstanceID], gocheck.IsNil)
}

// tests that apply instance marks the instance as
// executed, and moves it into the executed container
func (s *ApplyInstanceTest) TestBookKeeping(c *gocheck.C) {
	instance := s.scope.makeInstance(s.getInstructions(5))
	iid := instance.InstanceID
	s.scope.commitInstance(instance)

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
func (s *ApplyInstanceTest) TestUncommittedFailure(c *gocheck.C) {
	instance := s.scope.makeInstance(s.getInstructions(5))
	iid := instance.InstanceID
	s.scope.acceptInstance(instance)

	// sanity check
	c.Check(s.scope.inProgress, instMapContainsKey, iid)
	c.Check(s.scope.executed, gocheck.Not(instIdSliceContains), iid)
	c.Check(instance.Status, gocheck.Equals, INSTANCE_ACCEPTED)

	_, err := s.scope.applyInstance(instance)
	c.Assert(err, gocheck.NotNil)
}
