/**
tests the execution of committed commands
*/
package consensus

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

import (
	"launchpad.net/gocheck"
)

import (
	"testing_helpers"
	"store"
)

// tests the successful execution of commands
// for instances where all of the dependencies
// have been executed
func TestExecutionSuccessCase(t *testing.T) {

}

// tests that executing an instance moves the instance
// from committed to executed
func TestExecuteMovesInstance(t *testing.T) {

}

func TestExecutionUnexecutedDependencies(t *testing.T) {

}

func TestExecutionUncommittedDependencies(t *testing.T) {

}

func TestExecutionOrdering(t *testing.T) {
	manager := NewManager(newMockCluster())
	scope := NewScope("a", manager)
	if !testing_helpers.AssertEqual(t, "Initial instances size", 0, len(scope.instances)) { t.FailNow() }
	addInst := func() *Instance {
		inst := scope.makeInstance(getBasicInstruction())
		scope.preAcceptInstanceUnsafe(inst)
		scope.commitInstanceUnsafe(inst)
		return inst
	}
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

	expected := []InstanceID{
		i0.InstanceID,
		i1.InstanceID,
		i2.InstanceID,
		i3.InstanceID,
		i4.InstanceID,
		i5.InstanceID,
	}
	actual := scope.getExecutionOrder(i5)
	if !testing_helpers.AssertEqual(t, "Execution Order Length", len(expected), len(actual)) {
		t.FailNow()
	}

	for i := range expected {
		testing_helpers.AssertEqual(t, fmt.Sprintf("Execution Order [%v]", i), expected[i], actual[i])
	}
}

type ExecuteDependencyChanTest struct {
	baseScopeTest
}

var _ = gocheck.Suite(&ExecuteDependencyChanTest{})

// tests that instances up to and including the given
// instance are executed
func (s *ExecuteDependencyChanTest) TestSuccess(c *gocheck.C) {

}

// tests that an error is returned if an uncommitted instance id is provided
func (s *ExecuteDependencyChanTest) TestUncommittedFailure(c *gocheck.C) {

}

// tests that execute dependency chain only executes up
// to the target instance
func (s *ExecuteDependencyChanTest) TestStopOnInstance(c *gocheck.C) {

}

// tests that instances are not executed twice
func (s *ExecuteDependencyChanTest) TestSkipExecuted(c *gocheck.C) {

}

// tests that unexecuted dependencies, where the command leader
// is the local node, waits for the owning goroutine to execute
// before continuing
func (s *ExecuteDependencyChanTest) TestUnexecutedLocal(c *gocheck.C) {

}

// tests that unexecuted dependencies, where the command leader
// is the local node, waits until the execute timeout before executing
func (s *ExecuteDependencyChanTest) TestUnexecutedLocalTimeout(c *gocheck.C) {

}

// tests that unexecuted dependencies, where the command leader
// is not the local node, executes dependencies as soon as it finds them
func (s *ExecuteDependencyChanTest) TestUnexecutedRemote(c *gocheck.C) {

}


type ApplyInstanceTest struct {
	baseScopeTest
}

var _ = gocheck.Suite(&ApplyInstanceTest{})

func (s *ApplyInstanceTest) getInstructions(values ...int) []*store.Instruction {
	instructions := make([]*store.Instruction, len(values))
	for i, val := range values {
		instructions[i] = store.NewInstruction("set", "a", []string{fmt.Sprintf("%v", val)}, time.Now())
	}
	return instructions
}

func (s *ApplyInstanceTest) TestSuccess(c *gocheck.C) {
	instance := s.scope.makeInstance(s.getInstructions(5))
	committed, err := s.scope.commitInstance(instance)
	c.Assert(committed, gocheck.Equals, true)
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
