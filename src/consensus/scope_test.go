package consensus

import (
	"testing"
//	"time"
)

import (
//	"store"
	"testing_helpers"
)
// tests that scope doesn't try to add executed instances
// if no instances have been executed yet
func TestGetDepsNoExecutions(t *testing.T) {
	scope := NewScope("a", nil)
	expectedDeps := []InstanceID{}
	for i:=0; i<4; i++ {
		id := NewInstanceID()
		expectedDeps = append(expectedDeps, id)
		scope.inProgress[id] = nil
	}
	for i:=0; i<4; i++ {
		id := NewInstanceID()
		expectedDeps = append(expectedDeps, id)
		scope.committed[id] = nil
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

func TestInstanceCreationPersistenceError(t *testing.T) {

}

func TestGetCurrentDeps(t *testing.T) {
	scope := NewScope("a", nil)
	expectedDeps := []InstanceID{}
	for i:=0; i<4; i++ {
		id := NewInstanceID()
		expectedDeps = append(expectedDeps, id)
		scope.inProgress[id] = nil
	}
	for i:=0; i<4; i++ {
		id := NewInstanceID()
		expectedDeps = append(expectedDeps, id)
		scope.committed[id] = nil
	}
	for i:=0; i<4; i++ {
		id := NewInstanceID()
		scope.executed = append(scope.executed, id)
		if i == 3 {
			expectedDeps = append(expectedDeps, id)
		}
	}

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
func TestGetNextSeq(t *testing.T) {
	scope := NewScope("a", nil)
	scope.maxSeq = 5
	nextSeq := scope.getNextSeqUnsafe()
	testing_helpers.AssertEqual(t, "nextSeq", uint64(6), nextSeq)
}

// test that instances are created properly
func TestInstanceCreation(t *testing.T) {
	// TODO: test max seq assignment and scope update
	// TODO: test dependency selection
	// TODO: test new instances are added to inProgress
	//	scope := NewScope("a", nil)
}

