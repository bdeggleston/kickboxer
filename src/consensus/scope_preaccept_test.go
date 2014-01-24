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

/** preAcceptInstance **/

func TestPreAcceptInstanceSuccess(t *testing.T) {
	scope := setupScope()
	instance := scope.makeInstance(getBasicInstruction())

	// sanity check
	if scope.instances.Contains(instance) {
		t.Fatalf("Unexpectedly found new instance in scope instances")
	}
	if scope.inProgress.Contains(instance) {
		t.Fatalf("Unexpectedly found new instance in scope inProgress")
	}
	if scope.committed.Contains(instance) {
		t.Fatalf("Unexpectedly found new instance in scope committed")
	}

	seq := scope.maxSeq
	if err := scope.preAcceptInstance(instance); err != nil {
		t.Fatalf("Error preaccepting instance: %v", err)
	}

	if !scope.instances.Contains(instance) {
		t.Fatalf("Expected to find new instance in scope instances")
	}
	if !scope.inProgress.Contains(instance) {
		t.Fatalf("Expected to find new instance in scope inProgress")
	}
	if scope.committed.Contains(instance) {
		t.Fatalf("Unexpectedly found new instance in scope committed")
	}

	testing_helpers.AssertEqual(t, "replica seq", seq + 1, instance.Sequence)
	testing_helpers.AssertEqual(t, "scope seq", seq + 1, scope.maxSeq)
}

func TestPreAcceptInstanceHigherStatusFailure(t *testing.T) {
	scope := setupScope()
	instance := scope.makeInstance(getBasicInstruction())
	instance.Status = INSTANCE_ACCEPTED
	scope.instances.Add(instance)
	scope.inProgress.Add(instance)

	// sanity check
	if !scope.instances.Contains(instance) {
		t.Fatalf("Expected to find new instance in scope instances")
	}
	if !scope.inProgress.Contains(instance) {
		t.Fatalf("Expected to find new instance in scope inProgress")
	}
	if scope.committed.Contains(instance) {
		t.Fatalf("Unexpectedly found new instance in scope committed")
	}

	if err := scope.preAcceptInstance(instance); err != nil {
		if _, ok := err.(InvalidStatusUpdateError); !ok {
			t.Fatalf("Error preaccepting instance: %v", err)
		} else {
			t.Log("InvalidStatusUpdateError returned as expected")

		}
	}

	testing_helpers.AssertEqual(t, "Status", INSTANCE_ACCEPTED, instance.Status)
}

/** Leader **/

// tests all replicas returning results
func TestSendPreAcceptSuccess(t *testing.T) {
	nodes := setupReplicaSet(5)
	leader := nodes[0]
	replicas := nodes[1:]
	scope := leader.manager.getScope("a")
	instance := scope.makeInstance(getBasicInstruction())

	// all replicas agree
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		newInst := copyInstance(instance)
		return &PreAcceptResponse{
			Accepted:         true,
			MaxBallot:        newInst.MaxBallot,
			Instance:         newInst,
			MissingInstances: []*Instance{},
		}, nil
	}

	for _, replica := range replicas {
		replica.messageHandler = responseFunc
	}

	responses, err := scope.sendPreAccept(instance, transformMockNodeArray(replicas))
	if err != nil {
		t.Errorf("Unexpected error receiving responses: %v", err)
	}
	if len(responses) < 2 {
		t.Errorf("Less than quorum received")
	}

	// test that the nodes received the correct message
	for _, replica := range replicas {
		if !testing_helpers.AssertEqual(t, "num messages", 1, len(replica.sentMessages)) {
			continue
		}
		msg := replica.sentMessages[0]
		if _, ok := msg.(*PreAcceptRequest); !ok {
			t.Errorf("Wrong message type received: %T", msg)
		}
	}

}

func TestSendPreAcceptQuorumFailure(t *testing.T) {
	nodes := setupReplicaSet(5)
	leader := nodes[0]
	replicas := nodes[1:]
	scope := leader.manager.getScope("a")
	instance := scope.makeInstance(getBasicInstruction())

	// all replicas agree
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		newInst := copyInstance(instance)
		return &PreAcceptResponse{
			Accepted:         true,
			MaxBallot:        newInst.MaxBallot,
			Instance:         newInst,
			MissingInstances: []*Instance{},
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

	responses, err := scope.sendPreAccept(instance, transformMockNodeArray(replicas))
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
	if _, ok := err.(TimeoutError); !ok {
		t.Errorf("Expected TimeoutError, got: %T", err)
	}
	if responses != nil {
		t.Errorf("Expected nil responses, got: %v", responses)

	}
}

func TestSendPreAcceptBallotFailure(t *testing.T) {
	// TODO: figure out what to do in this situation
	// the only way this would happen if is the command
	// was taken over by another replica, in which case,
	// should we just wait for the other leader to
	// execute it?
	t.Skip("figure out the expected behavior")
}

func TestMergePreAcceptAttributes(t *testing.T) {
	leader := setupReplicaSet(1)[0]
	scope := leader.manager.getScope("a")
	instance := scope.makeInstance(getBasicInstruction())

	for i := 0; i < 4; i++ {
		instance.Dependencies = append(instance.Dependencies, NewInstanceID())
	}
	instance.Sequence = 3
	expected := NewInstanceIDSet(instance.Dependencies)

	remoteInstance := copyInstance(instance)
	remoteInstance.Dependencies = instance.Dependencies[1:]
	remoteInstance.Dependencies = append(remoteInstance.Dependencies, NewInstanceID())
	expected.Add(remoteInstance.Dependencies...)

	if size := len(instance.Dependencies); size != 4 {
		t.Fatalf("Expected 4 dependencies, got: %v", size)
	}
	if size := len(remoteInstance.Dependencies); size != 4 {
		t.Fatalf("Expected 4 dependencies, got: %v", size)
	}
	remoteInstance.Sequence++

	testing_helpers.AssertEqual(t, "instance sequence", uint64(3), instance.Sequence)
	testing_helpers.AssertEqual(t, "remote sequence", uint64(4), remoteInstance.Sequence)

	responses := []*PreAcceptResponse{&PreAcceptResponse{
		Accepted:         true,
		MaxBallot:        remoteInstance.MaxBallot,
		Instance:         remoteInstance,
		MissingInstances: []*Instance{},
	}}
	changes, err := scope.mergePreAcceptAttributes(instance, responses)

	if err != nil {
		t.Fatalf("There was a problem merging attributes: %v", err)
	}
	if !changes {
		t.Errorf("Expected changes to be reported")
	}
	if size := len(instance.Dependencies); size != 5 {
		t.Fatalf("Expected 5 dependencies, got: %v", size)
	}
	testing_helpers.AssertEqual(t, "instance sequence", uint64(4), instance.Sequence)

	// test dependencies
	actual := NewInstanceIDSet(instance.Dependencies)
	if !expected.Equal(actual) {
		t.Errorf("Actual dependencies do not match expected dependencies.\nExpected: %v\nGot: %v", expected, actual)
	}
}

func TestMergePreAcceptAttributesNoChanges(t *testing.T) {
	leader := setupReplicaSet(1)[0]
	scope := leader.manager.getScope("a")
	instance := scope.makeInstance(getBasicInstruction())

	for i := 0; i < 4; i++ {
		instance.Dependencies = append(instance.Dependencies, NewInstanceID())
	}
	instance.Sequence = 3
	expected := NewInstanceIDSet(instance.Dependencies)

	remoteInstance := copyInstance(instance)

	if size := len(instance.Dependencies); size != 4 {
		t.Fatalf("Expected 4 dependencies, got: %v", size)
	}
	if size := len(remoteInstance.Dependencies); size != 4 {
		t.Fatalf("Expected 4 dependencies, got: %v", size)
	}

	testing_helpers.AssertEqual(t, "instance sequence", uint64(3), instance.Sequence)
	testing_helpers.AssertEqual(t, "remote sequence", uint64(3), remoteInstance.Sequence)

	responses := []*PreAcceptResponse{&PreAcceptResponse{
		Accepted:         true,
		MaxBallot:        remoteInstance.MaxBallot,
		Instance:         remoteInstance,
		MissingInstances: []*Instance{},
	}}
	changes, err := scope.mergePreAcceptAttributes(instance, responses)

	if err != nil {
		t.Fatalf("There was a problem merging attributes: %v", err)
	}
	if changes {
		t.Errorf("Expected no changes to be reported")
	}
	if size := len(instance.Dependencies); size != 4 {
		t.Fatalf("Expected 4 dependencies, got: %v", size)
	}
	testing_helpers.AssertEqual(t, "instance sequence", uint64(3), instance.Sequence)

	// test dependencies
	actual := NewInstanceIDSet(instance.Dependencies)
	if !expected.Equal(actual) {
		t.Errorf("Actual dependencies do not match expected dependencies.\nExpected: %v\nGot: %v", expected, actual)
	}
}

/** Replica **/

// tests that the dependency match flag is set
// if the seq and deps matched
func TestHandlePreAcceptSameDeps(t *testing.T) {
	scope := setupScope()
	scope.maxSeq = 3

	instance := &Instance{
		InstanceID:   NewInstanceID(),
		LeaderID:     node.NewNodeId(),
		Commands:     getBasicInstruction(),
		Dependencies: scope.getCurrentDepsUnsafe(),
		Sequence:     scope.maxSeq + 1,
		Status:       INSTANCE_PREACCEPTED,
	}
	request := &PreAcceptRequest{
		Scope:    scope.name,
		Instance: instance,
	}

	// process the preaccept message
	response, err := scope.HandlePreAccept(request)

	if err != nil {
		t.Fatalf("Error handling pre accept: %v", err)
	}

	testing_helpers.AssertEqual(t, "Accepted", true, response.Accepted)

	localInstance := scope.instances[instance.InstanceID]
	expectedDeps := NewInstanceIDSet(instance.Dependencies)
	actualDeps := NewInstanceIDSet(localInstance.Dependencies)
	if !expectedDeps.Equal(actualDeps) {
		t.Fatalf("actual dependencies don't match expected dependencies")
	}
	testing_helpers.AssertEqual(t, "Sequence", uint64(4), localInstance.Sequence)
	testing_helpers.AssertEqual(t, "dependencyMatch", true, localInstance.dependencyMatch)
	testing_helpers.AssertEqual(t, "MissingInstances size", 0, len(response.MissingInstances))
}

// tests that the replica updates the sequence and
// dependencies if it disagrees with the leader
func TestHandlePreAcceptDifferentDepsAndSeq(t *testing.T) {
	scope := setupScope()
	scope.maxSeq = 3

	replicaDeps := scope.getCurrentDepsUnsafe()
	leaderDeps := scope.getCurrentDepsUnsafe()
	missingDep := leaderDeps[0]
	extraDep := NewInstanceID()
	leaderDeps[0] = extraDep
	instance := &Instance{
		InstanceID:   NewInstanceID(),
		LeaderID:     node.NewNodeId(),
		Commands:     getBasicInstruction(),
		Dependencies: leaderDeps,
		Sequence:     3,
		Status:       INSTANCE_PREACCEPTED,
	}
	request := &PreAcceptRequest{
		Scope:    scope.name,
		Instance: instance,
	}

	scope.instances[missingDep] = &Instance{InstanceID: missingDep}

	// process the preaccept message
	response, err := scope.HandlePreAccept(request)

	if err != nil {
		t.Fatalf("Error handling pre accept: %v", err)
	}

	testing_helpers.AssertEqual(t, "Accepted", true, response.Accepted)

	responseInst := response.Instance
	expectedDeps := NewInstanceIDSet(replicaDeps)

	actualDeps := NewInstanceIDSet(responseInst.Dependencies)
	testing_helpers.AssertEqual(t, "deps size", len(expectedDeps), len(actualDeps))
	if !expectedDeps.Equal(actualDeps) {
		t.Fatalf("actual dependencies don't match expected dependencies.\nExpected: %v\nGot: %v", expectedDeps, actualDeps)
	}

	testing_helpers.AssertEqual(t, "Sequence", uint64(4), responseInst.Sequence)
	testing_helpers.AssertEqual(t, "dependencyMatch", false, responseInst.dependencyMatch)

	// check that handle pre-accept returns any missing
	// instance dependencies that the leader didn't include
	if size := len(response.MissingInstances); size != 1 {
		t.Fatalf("Expected 1 missing instance, got: %v", size)
	}
	testing_helpers.AssertEqual(t, "InstanceId", missingDep, response.MissingInstances[0].InstanceID)

}

func TestHandleNoopPreaccept(t *testing.T) {

}

// if the pre accept message reaches the replica
// after the command has been accepted, or committed
func TestHandlePreAcceptLate(t *testing.T) {
	// TODO: does the replica need to do anything besides ignore it?
	t.Skip("figure out the expected behavior")
}
