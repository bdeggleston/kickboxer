package cluster

import (
	"testing"
)

/** Consensus Manager **/

// test that the consensus leader can accurately
// determine if the local node is eligible to
// be the command leader
func TestCommandLeaderEligibility(t *testing.T) {

}

// tests that ineligible commands are forwarded
// to the correct nodes
func TestCommandLeaderForwarding(t *testing.T) {

}

// test that command leader forwarding is tolerant
// of replicas being down
func TestCommandLeaderForwardingFailureTolerance(t *testing.T) {

}

// test that the get or create instance method
// works does that
func TestInstanceRetrieval(t *testing.T) {

}

/** Consensus Instance **/

// tests that the instance object is able to return
// the proper replicas and quorum size for different
// consistency levels
func TestGetReplicas(t *testing.T) {

}

// tests that calling executeCommand on a command
// without any undecided dependencies mutates the
// store as expected, and sets the proper status
// on the command object
func TestExecuteCommandMutatesStore(t *testing.T) {

}

// tests that calling executeCommand on a command
// with undecided dependencies waits for / resolves
// the undecided commands properly before executing
func TestUndecidedDependenciesExecuteCommand(t *testing.T) {

}

// tests that adding a new dependency via the addDependency
// method sets the proper sequence number, appends the cmd,
// and sets dependencyMatch if checkDeps is not nil
func TestAddDependency(t *testing.T) {

}

func TestDependencyUnionNoop(t *testing.T) {

}

func TestDependencyUnionMismatch(t *testing.T) {

}

func TestDependencyUnionOutOfOrder(t *testing.T) {

}

func TestHandleMessageBallotCheck(t *testing.T) {

}

