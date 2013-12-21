package cluster

import (
	"testing"
)

/** Proposals **/

// test that receiving a pre accept request for a command
// that doesn't interfere with any existing commands results
// in an acceptance message
func TestNonInterferingInstanceRequest(t *testing.T) {

}

// test that receiving a pre accept request for a command
// that interferes with existing commands results in an
// acceptance message if the dependencies match
func TestInterferingInstanceMatchingDependenciesRequest(t *testing.T) {

}

// test that receiving a pre accept request for a command
// that interferes with existing commands results in a
// rejection message if there are missing dependencies
//
// The rejection message should include the replica's view
// of the dependencies
func TestInterferingInstanceMissingDependenciesRequest(t *testing.T) {

}

/** Commit **/

/**  **/
