package consensus

import (
	"fmt"
	"testing"
	"time"
)

import (
	"message"
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

}

func TestMergePreAcceptAttributes(t *testing.T) {

}


/** Replica **/

func TestHandlePreAcceptSameDeps(t *testing.T) {

}

func TestHandlePreAcceptOldBallotFailure(t *testing.T) {

}

func TestHandlePreAcceptDifferentDepsAndSeq(t *testing.T) {

}
