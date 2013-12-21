package cluster

import (
	"testing"
)

/** Proposals **/

// if the leader receives a quorum of accepts from
// the replicas, it should send a commit message
func TestSuccessfulPreAcceptResponse(t *testing.T) {

}

// if the leader receives a quorum of rejects from
// the replicas, it should update it's dependencies
// from the responses and update all replicas with
// the updated attributes, committing after a quorum
// have replied
func TestFailedPreAcceptResponse(t *testing.T) {

}

// tests that the command leader abandons a request
// if a quorum of nodes did not reply in time
func TestFailedQuorumResponse(t *testing.T) {

}

// tests that if a node receives a consensus query for
// keys it's not a replica of, it forwards the query
// to a node that is
func TestProposalsAreForwardedToReplica(t *testing.T) {

}
