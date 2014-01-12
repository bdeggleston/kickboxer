/**
tests the execution of committed commands
*/
package consensus

import (
	"fmt"
	"testing"
)

import (
	"testing_helpers"
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

}

func TestExecutionOrderingSequenceTieBreaking(t *testing.T) {
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
