package consensus

import (
	"testing"
	"time"
)

import (
	"launchpad.net/gocheck"
)

import (
	"store"
	"testing_helpers"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	gocheck.TestingT(t)
}


// test that instances are created properly
func TestInstanceCreation(t *testing.T) {
	// TODO: test new instances are added to inProgress
	scope := setupScope()
	instructions := []*store.Instruction{store.NewInstruction("set", "b", []string{}, time.Now())}
	instance := scope.makeInstance(instructions)

	testing_helpers.AssertEqual(t, "Ballot", 0, int(instance.MaxBallot))
	testing_helpers.AssertEqual(t, "LeaderID", scope.GetLocalID(), instance.LeaderID)
}

func TestGetCurrentDeps(t *testing.T) {
	scope := setupScope()
	expectedDeps := []InstanceID{}
	for dep := range scope.inProgress {
		expectedDeps = append(expectedDeps, dep)
	}
	for dep := range scope.committed {
		expectedDeps = append(expectedDeps, dep)
	}
	expectedDeps = append(expectedDeps, scope.executed[len(scope.executed)-1])

	// sanity checks
	testing_helpers.AssertEqual(t, "inProgress len", 4, len(scope.inProgress))
	testing_helpers.AssertEqual(t, "committed len", 4, len(scope.committed))
	testing_helpers.AssertEqual(t, "executed len", 4, len(scope.executed))

	expected := NewInstanceIDSet(expectedDeps)
	actual := NewInstanceIDSet(scope.getCurrentDepsUnsafe())

	testing_helpers.AssertEqual(t, "set lengths", len(expected), len(actual))
	if !expected.Equal(actual) {
		t.Errorf("Expected deps do not match actual deps.\nExpected: %v\nGot: %v", expected, actual)
	}
}

// tests that scope doesn't try to add executed instances
// if no instances have been executed yet
func TestGetDepsNoExecutions(t *testing.T) {
	scope := setupScope()
	scope.executed = []InstanceID{}
	expectedDeps := []InstanceID{}
	for dep := range scope.inProgress {
		expectedDeps = append(expectedDeps, dep)
	}
	for dep := range scope.committed {
		expectedDeps = append(expectedDeps, dep)
	}

	// sanity checks
	testing_helpers.AssertEqual(t, "inProgress len", 4, len(scope.inProgress))
	testing_helpers.AssertEqual(t, "committed len", 4, len(scope.committed))
	testing_helpers.AssertEqual(t, "executed len", 0, len(scope.executed))

	expected := NewInstanceIDSet(expectedDeps)
	actual := NewInstanceIDSet(scope.getCurrentDepsUnsafe())

	testing_helpers.AssertEqual(t, "set lengths", len(expected), len(actual))
	if !expected.Equal(actual) {
		t.Errorf("Expected deps do not match actual deps.\nExpected: %v\nGot: %v", expected, actual)
	}
}

func TestGetNextSeq(t *testing.T) {
	scope := NewScope("a", nil)
	scope.maxSeq = 5
	nextSeq := scope.getNextSeqUnsafe()
	testing_helpers.AssertEqual(t, "nextSeq", uint64(6), nextSeq)
}

// tests that the addMissingInstance method works properly
func TestAddMissingInstanceNotPreviouslySeen(t *testing.T) {
	// TODO: this
}
