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
