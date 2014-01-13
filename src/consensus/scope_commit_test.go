package consensus

import (
	"testing"
)

import (
	"message"
	"node"
	"testing_helpers"
)

/** commitInstance **/

// tests that an instance is marked as committed,
// added to the committed set, removed from the
// inProgress set, and persisted if it hasn't
// already been executed
func TestCommitInstanceSuccess(t *testing.T) {
	scope := setupScope()

	replicaInstance := makeInstance(node.NewNodeId(), makeDependencies(4))
	scope.maxSeq = 3
	replicaInstance.Sequence = scope.maxSeq
	scope.acceptInstance(replicaInstance)

	leaderInstance := copyInstance(replicaInstance)
	leaderInstance.Sequence++
	leaderInstance.Dependencies = append(leaderInstance.Dependencies, NewInstanceID())

	// sanity checks
	if !scope.inProgress.Contains(leaderInstance) {
		t.Fatalf("Expected to find instance in scope inProgress")
	}
	if scope.committed.Contains(leaderInstance) {
		t.Fatalf("Unexpectedly found instance in scope committed")
	}
	testing_helpers.AssertEqual(t, "replica deps", 4, len(replicaInstance.Dependencies))
	testing_helpers.AssertEqual(t, "replica seq", uint64(3), replicaInstance.Sequence)
	testing_helpers.AssertEqual(t, "scope seq", uint64(3), scope.maxSeq)

	if accepted, err := scope.commitInstance(leaderInstance); err != nil {
		t.Fatalf("Unexpected error accepting instance: %v", err)
	} else if !accepted {
		t.Error("Acceptance unexpectedly skipped")
	}

	// test move
	if scope.inProgress.Contains(leaderInstance) {
		t.Errorf("Unexpectedly found instance in scope inProgress")
	}
	if !scope.committed.Contains(leaderInstance) {
		t.Errorf("Expected to find instance in scope committed")
	}

	// test values
	testing_helpers.AssertEqual(t, "replica Status", INSTANCE_COMMITTED, replicaInstance.Status)
	testing_helpers.AssertEqual(t, "leader Status", INSTANCE_COMMITTED, leaderInstance.Status)
	testing_helpers.AssertEqual(t, "replica deps", 5, len(replicaInstance.Dependencies))
	testing_helpers.AssertEqual(t, "replica seq", uint64(4), replicaInstance.Sequence)
	testing_helpers.AssertEqual(t, "scope seq", uint64(4), scope.maxSeq)
}

// tests that an instance is marked as committed,
// added to the instances and committed set, and
// persisted if the instance hasn't been seen before
func TestCommitInstanceUnseenSuccess(t *testing.T) {
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

	if accepted, err := scope.commitInstance(leaderInstance); err != nil {
		t.Fatalf("Unexpected error accepting instance: %v", err)
	} else if !accepted {
		t.Error("Acceptance unexpectedly skipped")
	}
	if !scope.instances.Contains(leaderInstance) {
		t.Fatalf("Expected to find instance in scope instance")
	}
	if scope.inProgress.Contains(leaderInstance) {
		t.Fatalf("Unexpectedly fount instance in scope inProgress")
	}
	if !scope.committed.Contains(leaderInstance) {
		t.Fatalf("Expected to find instance in scope committed")
	}
	replicaInstance := scope.instances[leaderInstance.InstanceID]
	testing_helpers.AssertEqual(t, "replica Status", INSTANCE_COMMITTED, replicaInstance.Status)
	testing_helpers.AssertEqual(t, "leader Status", INSTANCE_COMMITTED, leaderInstance.Status)
	testing_helpers.AssertEqual(t, "replica deps", 4, len(replicaInstance.Dependencies))
	testing_helpers.AssertEqual(t, "replica seq", uint64(5), replicaInstance.Sequence)
	testing_helpers.AssertEqual(t, "scope seq", uint64(5), scope.maxSeq)
}

// tests that an instance is not marked as committed,
// or added to the committed set if it already has
// been executed
func TestCommitInstanceExecutedFailure(t *testing.T) {
	scope := setupScope()

	replicaInstance := makeInstance(node.NewNodeId(), makeDependencies(4))
	scope.maxSeq = 3
	replicaInstance.Sequence = scope.maxSeq
	replicaInstance.Status = INSTANCE_EXECUTED

	scope.instances.Add(replicaInstance)
	scope.executed = append(scope.executed, replicaInstance.InstanceID)

	leaderInstance := copyInstance(replicaInstance)
	leaderInstance.Status = INSTANCE_ACCEPTED

	// sanity checks
	if scope.committed.Contains(leaderInstance) {
		t.Fatalf("Unexpectedly found instance in scope committed")
	}
	if scope.inProgress.Contains(leaderInstance) {
		t.Fatalf("Unexpectedly found instance in scope inProgress")
	}
	if scope.executed[len(scope.executed) - 1] != replicaInstance.InstanceID {
		t.Fatalf("Expected to find instance in scope executed")
	}

	if accepted, err := scope.commitInstance(leaderInstance); err != nil {
		t.Fatalf("Unexpected error accepting instance: %v", err)
	} else if accepted {
		t.Error("Expected accept to be skipped")
	}

	// check set memberships haven't changed
	if scope.committed.Contains(leaderInstance) {
		t.Fatalf("Unexpectedly found instance in scope committed")
	}
	if scope.inProgress.Contains(leaderInstance) {
		t.Fatalf("Unexpectedly found instance in scope inProgress")
	}
	if scope.executed[len(scope.executed) - 1] != replicaInstance.InstanceID {
		t.Fatalf("Expected to find instance in scope executed")
	}
	testing_helpers.AssertEqual(t, "replica status", INSTANCE_EXECUTED, replicaInstance.Status)
}

/** leader **/

// tests that calling sendCommit sends commit requests
// to the other replicas
func TestSendCommitSuccess(t *testing.T) {
	nodes := setupReplicaSet(5)
	leader := nodes[0]
	replicas := nodes[1:]
	scope := leader.manager.getScope("a")
	instance := scope.makeInstance(getBasicInstruction())

	if err := scope.sendCommit(instance, transformMockNodeArray(replicas)); err != nil {
		t.Fatalf("Error sending commit message")
	}

	// all replicas agree
	responseWaitChan := make(chan bool, len(replicas))
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		responseWaitChan <- true
		return &CommitResponse{}, nil
	}

	for _, replica := range replicas {
		replica.messageHandler = responseFunc
	}

	// wait for all of the nodes to receive their messages
	for i:=0;i<len(replicas);i++ {
		<-responseWaitChan
	}
	// test that the nodes received the correct message
	for _, replica := range replicas {
		if !testing_helpers.AssertEqual(t, "num messages", 1, len(replica.sentMessages)) {
			continue
		}
		msg := replica.sentMessages[0]
		if _, ok := msg.(*CommitRequest); !ok {
			t.Errorf("Wrong message type received: %T", msg)
		}
	}

}

/** replica **/

// tests that an instance is marked as committed when
// a commit request is recived
func TestHandleCommitSuccess(t *testing.T) {
	scope := setupScope()
	instance := scope.makeInstance(getBasicInstruction())

	if success, err := scope.acceptInstance(instance); err != nil {
		t.Fatalf("Error preaccepting instance: %v", err)
	} else if !success {
		t.Fatalf("Preaccept was not successful")
	}

	// sanity check
	testing_helpers.AssertEqual(t, "Status", INSTANCE_ACCEPTED, instance.Status)

	request := &CommitRequest{
		Scope: scope.name,
		Instance: instance,
	}

	response, err := scope.HandleCommit(request)
	if err != nil {
		t.Fatalf("Error handling accept: %v", err)
	}

	if response == nil {
		t.Errorf("Got nil CommitResponse")
	}

	testing_helpers.AssertEqual(t, "Status", INSTANCE_COMMITTED, instance.Status)
}

// tests that commits are handled properly if
// the commit if for an instance the node has
// not previously seen
func TestHandleCommitUnknownInstance(t *testing.T) {
	scope := setupScope()

	leaderID := node.NewNodeId()
	leaderInstance := makeInstance(leaderID, scope.getCurrentDepsUnsafe())
	leaderInstance.Sequence += 5

	request := &CommitRequest{
		Scope: scope.name,
		Instance: leaderInstance,
	}

	if scope.instances.ContainsID(leaderInstance.InstanceID) {
		t.Errorf("Unexpectedly found leader instance in scope instances")
	}

	response, err := scope.HandleCommit(request)
	if err != nil {
		t.Fatalf("Error handling accept: %v", err)
	}
	if response == nil {
		t.Errorf("Got nil CommitResponse")
	}

	if !scope.instances.ContainsID(leaderInstance.InstanceID) {
		t.Errorf("Expected to find leader instance in scope instances")
	}
	instance := scope.instances[leaderInstance.InstanceID]

	testing_helpers.AssertEqual(t, "Status", INSTANCE_COMMITTED, instance.Status)
}

// when a commit message is received, the instance should
// be asynchronously executed against the store
func TestHandleCommitAsyncExecute(t *testing.T) {
	// TODO:
}
