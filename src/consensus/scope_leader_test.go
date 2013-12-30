/**
tests the command leaders protocol execution
*/
package consensus

import (
	"testing"
)

// tests that execution will fail if the command leader
// (the node running ExecuteInstructions) can't find
// itself in the list of given replicas
func TestNonReplicaLeaderFailure(t *testing.T) {

}

// tests that the comand leader aborts if there's an
// error creating a new instance
func TestInstanceCreationPersistenceError(t *testing.T) {

}
