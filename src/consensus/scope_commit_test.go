package consensus

import (
	"testing"
)

/** commitInstance **/

// tests that an instance is marked as committed,
// added to the committed set, removed from the
// inProgress set, and persisted if it hasn't
// already been executed
func TestCommitInstanceSuccess(t *testing.T) {

}

// tests that an instance is not marked as committed,
// or added to the committed set if it already has
// been executed
func TestCommitInstanceExecutedFailure(t *testing.T) {

}

/** leader **/

// tests that calling sendCommit sends commit requests
// to the other replicas
func TestSendCommitSuccess(t *testing.T) {

}

/** replica **/

// tests that an instance is marked as committed when
// a commit request is recived
func TestHandleCommitSuccess(t *testing.T) {

}

// tests that commits are handled properly if
// the commit if for an instance the node has
// not previously seen
func TestHandleCommitUnknownInstance(t *testing.T) {

}
