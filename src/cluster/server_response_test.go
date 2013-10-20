package cluster

import (
	"testing"
)

import (
	"testing_helpers"
)

func TestServerDiscoverPeersResponse(t *testing.T) {
	c := makeRing(5, 3)
	server := &PeerServer{cluster:c}

	msg := &DiscoverPeersRequest{NodeId:NewNodeId()}
	response, err := server.executeRequest(msg.NodeId, msg, msg.GetType())

	if err != nil {
		t.Fatalf("Unexpected error executing request: %v", err)
	}

	peerResponse, ok := response.(*DiscoverPeerResponse)
	if !ok {
		t.Fatalf("Unexpected response type: %T", response)
	}

	testing_helpers.AssertEqual(t, "num peers", len(c.getPeerData()), len(peerResponse.Peers))
}


