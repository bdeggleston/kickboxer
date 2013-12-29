package consensus

import (
	"fmt"
	"testing"
	"time"
)

import (
	"message"
	"testing_helpers"
)

/** Leader **/

// tests all replicas returning results
func TestSendPreAcceptSuccess(t *testing.T) {
	nodes := setupReplicaSet(5)
	leader := nodes[0]
	replicas := nodes[1:]
	scope := leader.manager.getScope("a")
	instance, err := scope.makeInstance(getBasicInstruction())
	if err != nil {
		t.Fatalf("There was a problem creating the instance: %v", err)
	}

	// all replicas agree
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		newInst := copyInstance(instance)
		return &PreAcceptResponse{
			Accepted: true,
			MaxBallot: newInst.MaxBallot,
			Instance: newInst,
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

}

func TestSendPreAcceptQuorumFailure(t *testing.T) {
	nodes := setupReplicaSet(5)
	leader := nodes[0]
	replicas := nodes[1:]
	scope := leader.manager.getScope("a")
	instance, err := scope.makeInstance(getBasicInstruction())
	if err != nil {
		t.Fatalf("There was a problem creating the instance: %v", err)
	}

	// all replicas agree
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		newInst := copyInstance(instance)
		return &PreAcceptResponse{
			Accepted: true,
			MaxBallot: newInst.MaxBallot,
			Instance: newInst,
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
	instance, err := scope.makeInstance(getBasicInstruction())

	if err != nil {
		t.Fatalf("There was a problem creating the instance: %v", err)
	}
	for i:=0;i<4;i++ {
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
		Accepted: true,
		MaxBallot: remoteInstance.MaxBallot,
		Instance: remoteInstance,
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
	instance, err := scope.makeInstance(getBasicInstruction())

	if err != nil {
		t.Fatalf("There was a problem creating the instance: %v", err)
	}
	for i:=0;i<4;i++ {
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
		Accepted: true,
		MaxBallot: remoteInstance.MaxBallot,
		Instance: remoteInstance,
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

func TestHandlePreAcceptSameDeps(t *testing.T) {

}

func TestHandlePreAcceptOldBallotFailure(t *testing.T) {

}

func TestHandlePreAcceptDifferentDepsAndSeq(t *testing.T) {

}

// if the pre accept message reaches the replica
// after the command has been accepted, or committed
func TestHandlePreAcceptLate(t *testing.T) {
	// TODO: does the replica need to do anything besides ignore it?
	t.Skip("figure out the expected behavior")
}
