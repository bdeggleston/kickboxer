package consensus

import (
	"fmt"
	"testing"
	"time"
)

import (
	"message"
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
	testing_helpers.AssertEqual(t, "replica Status", INSTANCE_ACCEPTED, replicaInstance.Status)
	testing_helpers.AssertEqual(t, "leader Status", INSTANCE_ACCEPTED, leaderInstance.Status)
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
	if scope.instances.Contains(leaderInstance) {
		t.Fatalf("Unexpectedly found instance in scope instances")
	}
	if scope.inProgress.Contains(leaderInstance) {
		t.Fatalf("Unexpectedly found instance in scope inProgress")
	}
	if scope.committed.Contains(leaderInstance) {
		t.Fatalf("Unexpectedly found instance in scope committed")
	}

	if accepted, err := scope.acceptInstance(leaderInstance); err != nil {
		t.Fatalf("Unexpected error accepting instance: %v", err)
	} else if !accepted {
		t.Error("Acceptance unexpectedly skipped")
	}
	if !scope.instances.Contains(leaderInstance) {
		t.Fatalf("Expected to find instance in scope instance")
	}
	if !scope.inProgress.Contains(leaderInstance) {
		t.Fatalf("Expected to find instance in scope inProgress")
	}
	if scope.committed.Contains(leaderInstance) {
		t.Fatalf("Unexpectedly found instance in scope committed")
	}
	replicaInstance := scope.instances[leaderInstance.InstanceID]
	testing_helpers.AssertEqual(t, "replica Status", INSTANCE_ACCEPTED, replicaInstance.Status)
	testing_helpers.AssertEqual(t, "leader Status", INSTANCE_ACCEPTED, leaderInstance.Status)
	testing_helpers.AssertEqual(t, "replica deps", 4, len(replicaInstance.Dependencies))
	testing_helpers.AssertEqual(t, "replica seq", uint64(5), replicaInstance.Sequence)
	testing_helpers.AssertEqual(t, "scope seq", uint64(5), scope.maxSeq)
}

// tests that an instance is not marked as accepted,
// or added to the inProgress set if it already has
// a higher status
func TestAcceptInstanceHigherStatusFailure(t *testing.T) {
	scope := setupScope()

	replicaInstance := makeInstance(node.NewNodeId(), makeDependencies(4))
	scope.maxSeq = 3
	replicaInstance.Sequence = scope.maxSeq
	replicaInstance.Status = INSTANCE_COMMITTED

	scope.instances.Add(replicaInstance)
	scope.committed.Add(replicaInstance)

	leaderInstance := copyInstance(replicaInstance)
	leaderInstance.Status = INSTANCE_ACCEPTED

	// sanity checks
	if _, exists := scope.committed[leaderInstance.InstanceID]; !exists {
		t.Fatalf("Expected to find instance in scope committed")
	}
	if _, exists := scope.inProgress[leaderInstance.InstanceID]; exists {
		t.Fatalf("Unexpectedly found instance in scope inProgress")
	}

	if accepted, err := scope.acceptInstance(leaderInstance); err != nil {
		t.Fatalf("Unexpected error accepting instance: %v", err)
	} else if accepted {
		t.Error("Expected accept to be skipped")
	}

	// check set memberships haven't changed
	if _, exists := scope.committed[leaderInstance.InstanceID]; !exists {
		t.Fatalf("Expected to find instance in scope committed")
	}
	if _, exists := scope.inProgress[leaderInstance.InstanceID]; exists {
		t.Fatalf("Unexpectedly found instance in scope inProgress")
	}
	testing_helpers.AssertEqual(t, "replica status", INSTANCE_COMMITTED, replicaInstance.Status)
}

/** leader **/

// tests all replicas returning results
func TestSendAcceptSuccess(t *testing.T) {
	nodes := setupReplicaSet(5)
	leader := nodes[0]
	replicas := nodes[1:]
	scope := leader.manager.getScope("a")
	instance := scope.makeInstance(getBasicInstruction())

	if accepted, err := scope.preAcceptInstance(instance); err != nil {
		t.Fatalf("Unexpected error pre accepting instance: %v", err)
	} else if !accepted {
		t.Error("Pre accept unexpectedly skipped")
	}
	if accepted, err := scope.acceptInstance(instance); err != nil {
		t.Fatalf("Unexpected error accepting instance: %v", err)
	} else if !accepted {
		t.Error("Accept unexpectedly skipped")
	}

	// all replicas agree
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		return &AcceptResponse{
			Accepted:         true,
			MaxBallot:        instance.MaxBallot,
		}, nil
	}

	for _, replica := range replicas {
		replica.messageHandler = responseFunc
	}

	err := scope.sendAccept(instance, transformMockNodeArray(replicas))
	if err != nil {
		t.Errorf("Unexpected error receiving responses: %v", err)
	}

	// test that the nodes received the correct message
	for _, replica := range replicas {
		testing_helpers.AssertEqual(t, "num messages", 1, len(replica.sentMessages))
		msg := replica.sentMessages[0]
		if _, ok := msg.(*AcceptRequest); !ok {
			fmt.Errorf("Wrong message type received: %T", msg)
		}
	}
}

// tests proper error is returned if
// less than a quorum respond
func TestSendAcceptQuorumFailure(t *testing.T) {
	oldTimeout := ACCEPT_TIMEOUT
	ACCEPT_TIMEOUT = 50
	defer func() { ACCEPT_TIMEOUT = oldTimeout }()

	nodes := setupReplicaSet(5)
	leader := nodes[0]
	replicas := nodes[1:]
	scope := leader.manager.getScope("a")
	instance := scope.makeInstance(getBasicInstruction())

	if accepted, err := scope.preAcceptInstance(instance); err != nil {
		t.Fatalf("Unexpected error pre accepting instance: %v", err)
	} else if !accepted {
		t.Error("Pre accept unexpectedly skipped")
	}
	if accepted, err := scope.acceptInstance(instance); err != nil {
		t.Fatalf("Unexpected error accepting instance: %v", err)
	} else if !accepted {
		t.Error("Accept unexpectedly skipped")
	}

	// all replicas agree
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		return &AcceptResponse{
			Accepted:         true,
			MaxBallot:        instance.MaxBallot,
		}, nil
	}
	hangResponse := func(n *mockNode, m message.Message) (message.Message, error) {
		time.Sleep(1 * time.Second)
		return nil, fmt.Errorf("nope")
	}

	for i, replica := range replicas {
		if i == 0 {
			replica.messageHandler = responseFunc
		} else {
			replica.messageHandler = hangResponse
		}
	}
	err := scope.sendAccept(instance, transformMockNodeArray(replicas))
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
	if _, ok := err.(TimeoutError); !ok {
		t.Errorf("Expected TimeoutError, got: %T", err)
	}
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
	scope := setupScope()
	instance := scope.makeInstance(getBasicInstruction())

	if success, err := scope.preAcceptInstance(instance); err != nil {
		t.Fatalf("Error preaccepting instance: %v", err)
	} else if !success {
		t.Fatalf("Preaccept was not successful")
	}

	leaderInstance := copyInstance(instance)
	leaderInstance.Dependencies = append(leaderInstance.Dependencies, NewInstanceID())
	leaderInstance.Sequence += 5
	leaderInstance.MaxBallot++

	request := &AcceptRequest{
		Scope: scope.name,
		Instance: leaderInstance,
		MissingInstances: []*Instance{},
	}

	response, err := scope.HandleAccept(request)
	if err != nil {
		t.Fatalf("Error handling accept: %v", err)
	}

	testing_helpers.AssertEqual(t, "Accepted", true, response.Accepted)

	// check dependencies
	expectedDeps := NewInstanceIDSet(leaderInstance.Dependencies)
	actualDeps := NewInstanceIDSet(instance.Dependencies)
	testing_helpers.AssertEqual(t, "deps size", len(expectedDeps), len(actualDeps))
	if !expectedDeps.Equal(actualDeps) {
		t.Fatalf("actual dependencies don't match expected dependencies.\nExpected: %v\nGot: %v", expectedDeps, actualDeps)
	}

	testing_helpers.AssertEqual(t, "Sequence", leaderInstance.Sequence, instance.Sequence)
	testing_helpers.AssertEqual(t, "Sequence", leaderInstance.Sequence, scope.maxSeq)
}

// tests that accepts are handled properly if
// the commit if for an instance the node has
// not been previously seen by this replica
func TestHandleAcceptUnknownInstance(t *testing.T) {
	scope := setupScope()

	leaderID := node.NewNodeId()
	leaderInstance := makeInstance(leaderID, scope.getCurrentDepsUnsafe())
	leaderInstance.Sequence += 5

	request := &AcceptRequest{
		Scope: scope.name,
		Instance: leaderInstance,
		MissingInstances: []*Instance{},
	}

	// sanity checks
	if scope.instances.ContainsID(leaderInstance.InstanceID) {
		t.Fatalf("Unexpectedly found instance in scope instances")
	}

	response, err := scope.HandleAccept(request)
	if err != nil {
		t.Fatalf("Error handling accept: %v", err)
	}

	if !scope.instances.ContainsID(leaderInstance.InstanceID) {
		t.Fatalf("Expected instance in scope instances")
	}
	instance := scope.instances[leaderInstance.InstanceID]

	testing_helpers.AssertEqual(t, "Accepted", true, response.Accepted)

	// check dependencies
	expectedDeps := NewInstanceIDSet(leaderInstance.Dependencies)
	actualDeps := NewInstanceIDSet(instance.Dependencies)
	testing_helpers.AssertEqual(t, "deps size", len(expectedDeps), len(actualDeps))
	if !expectedDeps.Equal(actualDeps) {
		t.Fatalf("actual dependencies don't match expected dependencies.\nExpected: %v\nGot: %v", expectedDeps, actualDeps)
	}

	testing_helpers.AssertEqual(t, "Sequence", leaderInstance.Sequence, instance.Sequence)
	testing_helpers.AssertEqual(t, "Sequence", leaderInstance.Sequence, scope.maxSeq)
}

// tests that accept messages fail if an higher
// ballot number has been seen for this message
func TestHandleAcceptOldBallotFailure(t *testing.T) {
	scope := setupScope()
	instance := scope.makeInstance(getBasicInstruction())

	if success, err := scope.preAcceptInstance(instance); err != nil {
		t.Fatalf("Error preaccepting instance: %v", err)
	} else if !success {
		t.Fatalf("Preaccept was not successful")
	}

	leaderInstance := copyInstance(instance)
	leaderInstance.Sequence += 5

	request := &AcceptRequest{
		Scope: scope.name,
		Instance: leaderInstance,
		MissingInstances: []*Instance{},
	}

	instance.MaxBallot++
	response, err := scope.HandleAccept(request)
	if err != nil {
		t.Fatalf("Error handling accept: %v", err)
	}

	testing_helpers.AssertEqual(t, "Accepted", false, response.Accepted)
	testing_helpers.AssertEqual(t, "MaxBallot", instance.MaxBallot, response.MaxBallot)
}

// tests that handle accept adds any missing instances
// in the missing instances message
func TestHandleAcceptMissingInstanceBehavior(t *testing.T) {
	scope := setupScope()
	instance := scope.makeInstance(getBasicInstruction())

	if success, err := scope.preAcceptInstance(instance); err != nil {
		t.Fatalf("Error preaccepting instance: %v", err)
	} else if !success {
		t.Fatalf("Preaccept was not successful")
	}

	leaderID := node.NewNodeId()
	missingInstance := makeInstance(leaderID, instance.Dependencies)
	leaderInstance := copyInstance(instance)
	leaderInstance.Dependencies = append(leaderInstance.Dependencies, missingInstance.InstanceID)
	leaderInstance.Sequence += 5
	leaderInstance.MaxBallot++

	// sanity checks
	if scope.instances.ContainsID(missingInstance.InstanceID) {
		t.Fatalf("Unexpectedly found missing instance in scope instances")
	}

	request := &AcceptRequest{
		Scope: scope.name,
		Instance: leaderInstance,
		MissingInstances: []*Instance{missingInstance},
	}

	response, err := scope.HandleAccept(request)
	if err != nil {
		t.Fatalf("Error handling accept: %v", err)
	}
	testing_helpers.AssertEqual(t, "Accepted", true, response.Accepted)
	if !scope.instances.ContainsID(missingInstance.InstanceID) {
		t.Fatalf("Expected instance in scope instances")
	}

}
