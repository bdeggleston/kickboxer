package consensus

import (
	"testing"
)

import (
	"message"
)

/** Leader **/

// tests all replicas returning results
func TestSendPreAcceptSuccess(t *testing.T) {
	setupTestLogging()
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
