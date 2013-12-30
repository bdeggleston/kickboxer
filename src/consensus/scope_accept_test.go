package consensus

import (
	"testing"
)

import (
	"node"
	"testing_helpers"
)

/** acceptInstance **/

// tests that an instance is marked as accepted,
// added to the inProgress set, has it's seq & deps
// updated and persisted if it's only preaccepted
func TestAcceptInstanceSuccess(t *testing.T) {
	scope := setupScope()

	replicaInstance := makeInstance(node.NewNodeId(), makeDependencies(4))
	scope.maxSeq = 3
	replicaInstance.Sequence = scope.maxSeq

	scope.instances.Add(replicaInstance)
	scope.inProgress.Add(replicaInstance)
	scope.maxSeq = replicaInstance.Sequence

	// sanity checks
	testing_helpers.AssertEqual(t, "replica deps", 4, len(replicaInstance.Dependencies))
	testing_helpers.AssertEqual(t, "replica seq", uint64(3), replicaInstance.Sequence)
	testing_helpers.AssertEqual(t, "scope seq", uint64(3), scope.maxSeq)

	leaderInstance := copyInstance(replicaInstance)
	leaderInstance.Sequence++
	leaderInstance.Dependencies = append(leaderInstance.Dependencies, NewInstanceID())

	if accepted, err := scope.acceptInstance(leaderInstance); err != nil {
		t.Fatalf("Unexpected error accepting instance: %v", err)
	} else if !accepted {
		t.Error("Acceptance unexpectedly skipped")
	}
	testing_helpers.AssertEqual(t, "replica deps", 5, len(replicaInstance.Dependencies))
	testing_helpers.AssertEqual(t, "replica seq", uint64(4), replicaInstance.Sequence)
	testing_helpers.AssertEqual(t, "scope seq", uint64(4), scope.maxSeq)

}

// tests that an instance is marked as accepted,
// added to the instances and inProgress set, and
// persisted if the instance hasn't been seen before
func TestAcceptInstanceUnseenSuccess(t *testing.T) {
	scope := setupScope()
	scope.maxSeq = 3

	leaderInstance := makeInstance(node.NewNodeId(), makeDependencies(4))
	leaderInstance.Sequence = scope.maxSeq + 2

	// sanity checks
	if _, exists := scope.instances[leaderInstance.InstanceID]; exists {
		t.Fatalf("Unexpectedly found instance in scope instances")
	}
	if _, exists := scope.inProgress[leaderInstance.InstanceID]; exists {
		t.Fatalf("Unexpectedly found instance in scope inProgress")
	}
	if _, exists := scope.committed[leaderInstance.InstanceID]; exists {
		t.Fatalf("Unexpectedly found instance in scope committed")
	}

	if accepted, err := scope.acceptInstance(leaderInstance); err != nil {
		t.Fatalf("Unexpected error accepting instance: %v", err)
	} else if !accepted {
		t.Error("Acceptance unexpectedly skipped")
	}
	if _, exists := scope.instances[leaderInstance.InstanceID]; !exists {
		t.Fatalf("Expected to find instance in scope instance")
	}
	if _, exists := scope.inProgress[leaderInstance.InstanceID]; !exists {
		t.Fatalf("Expected to find instance in scope inProgress")
	}
	if _, exists := scope.committed[leaderInstance.InstanceID]; exists {
		t.Fatalf("Unexpectedly found instance in scope committed")
	}
	replicaInstance := scope.instances[leaderInstance.InstanceID]
	testing_helpers.AssertEqual(t, "replica deps", 4, len(replicaInstance.Dependencies))
	testing_helpers.AssertEqual(t, "replica seq", uint64(5), replicaInstance.Sequence)
	testing_helpers.AssertEqual(t, "scope seq", uint64(5), scope.maxSeq)
}

// tests that an instance is not marked as accepted,
// or added to the inProgress set if it already has
// a higher status
func TestAcceptInstanceHigherStatusFailure(t *testing.T) {

}

/** leader **/

// tests all replicas returning results
func TestSendAcceptSuccess(t *testing.T) {

}

// tests proper error is returned if
// less than a quorum respond
func TestSendAcceptQuorumFailure(t *testing.T) {

}

func TestSendAcceptBallotFailure(t *testing.T) {
	// TODO: figure out what to do in this situation
	// the only way this would happen if is the command
	// was taken over by another replica, in which case,
	// should we just wait for the other leader to
	// execute it?
	t.Skip("figure out the expected behavior")
}

/** replica **/

// test that instances are marked as accepted when
// an accept request is received, and there are no
// problems with the request
func TestHandleAcceptSuccessCase(t *testing.T) {

}

// tests that accept messages fail if an higher
// ballot number has been seen for this message
func TestHandleAcceptOldBallotFailure(t *testing.T) {

}

// tests that handle accept adds any missing instances
// in the missing instances message
func TestHandleAcceptMissingInstanceBehavior(t *testing.T) {

}

// tests that accepts are handled properly if
// the commit if for an instance the node has
// not been previously seen by this replica
func TestHandleAcceptUnknownInstance(t *testing.T) {

}
