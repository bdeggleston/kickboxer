package cluster

import (
	"testing"
)

// tests streamFromNode methods sends a StreamDataRequest
// to the requesting node
func TestStreamFromNodeMethod(t *testing.T) {

}

/************** query behavior tests **************/

// writes should be routed to the streaming node and mirrored to the local node
func TestStreamingWriteRouting(t *testing.T) {

}

// reads should be routed directly to the streaming nodes
func TestStreamingReadRouting(t *testing.T) {

}

/************** streaming data tests **************/

