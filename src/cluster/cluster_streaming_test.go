package cluster

import (
	"testing"
)

// tests streamFromNode method
func TestStreamFromNodeMethod(t *testing.T) {
	// TODO: test StreamRequest is sent

	// TODO: test status is set to CLUSTER_STREAMING

}

// tests that method panics if the given node is not a remote node
func TestStreamFromNodeMethodWithNonRemoteNode(t *testing.T) {

}

/************** query behavior tests **************/

// writes should be routed to the streaming node and mirrored to the local node
func TestStreamingWriteRouting(t *testing.T) {

}

// reads should be routed directly to the streaming nodes
func TestStreamingReadRouting(t *testing.T) {

}

/************** streaming data tests **************/

