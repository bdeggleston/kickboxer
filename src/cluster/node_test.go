package cluster

import (
	"testing"
)

import (
	"testing_helpers"
)


/************** local tests **************/

// tests that starting the local node starts the local store
func TestStartingConnectsToStore(t *testing.T) {

}

// tests that calling ExecuteRead calls the proper store method
func TestReadRequestCallsCorrectStoreMethod(t *testing.T) {

}

// tests that calling ExecuteWrite calls the proper store method
func TestWriteRequestCallsCorrectStoreMethod(t *testing.T) {

}

/************** remote tests **************/

// tests that calling Start on a remote node initiates
// a connection to the remote peer server, copies
// it's data, and sets the node's status to UP
func TestStartingConnectsToPeer(t *testing.T) {
	// write a connection acceptance message
	sock := newBiConn(1, 1)
	response := &ConnectionAcceptedResponse{
		NodeId:NewNodeId(),
		Name:"Ghost",
		Token:Token([]byte{0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,3}),
	}
	WriteMessage(sock.input[0], response)

	cluster := setupCluster()
	node := NewRemoteNode("127.0.0.2:9998", cluster)
	conn := &Connection{socket:sock}
	node.pool.Put(conn)

	// pre startup sanity check
	testing_helpers.AssertEqual(t, "pre start conn status", false, conn.completedHandshake)
	testing_helpers.AssertEqual(t, "pre start name", "", node.name)
	testing_helpers.AssertEqual(t, "pre start id", NodeId(""), node.id)
	testing_helpers.AssertEqual(t, "pre start status", NODE_INITIALIZING, node.status)
	testing_helpers.AssertSliceEqual(t, "pre start token", nil, node.token)

	// start the node
	err := node.Start()
	if err != nil {
		t.Fatalf("Unexpected error of type %T: %v", err, err)
	}

	// check that values were saved properly
	testing_helpers.AssertEqual(t, "post start conn status", true, conn.completedHandshake)
	testing_helpers.AssertEqual(t, "post start name", response.Name, node.name)
	testing_helpers.AssertEqual(t, "post start id", response.NodeId, node.id)
	testing_helpers.AssertEqual(t, "post start status", NODE_UP, node.status)
	testing_helpers.AssertSliceEqual(t, "post start token", response.Token, node.token)


}

// tests that sending and receiving messages works as
// expected
func TestMessageSendingSuccessCase(t *testing.T) {
	sock := newBiConn(1, 1)
	expected := &DiscoverPeerResponse{Peers:[]*PeerData{}}

	WriteMessage(sock.input[0], expected)

	cluster := setupCluster()
	node := NewRemoteNode("127.0.0.2:9998", cluster)
	node.status = NODE_UP
	conn := &Connection{socket:sock}
	conn.SetHandshakeCompleted()
	node.pool.Put(conn)

	request := &DiscoverPeersRequest{NodeId:node.GetId()}
	rawResponse, mtype, err := node.sendMessage(request)
	if err != nil {
		t.Fatalf("Unexpected error of type [%T]: %v", err, err)
	}
	if mtype != DISCOVER_PEERS_RESPONSE {
		t.Errorf("Unexpected message response type. Expected %v, got %v", DISCOVER_PEERS_RESPONSE, mtype)
	}
	response, ok := rawResponse.(*DiscoverPeerResponse)
	if !ok {
		t.Fatalf("Unexpected message type. Expected DiscoverPeerResponse, got %T", rawResponse)
	}
	if len(response.Peers) != 0 {
		t.Errorf("Unexpected response data length. Expected 0, got %v", len(response.Peers))
	}

	if node.status != NODE_UP {
		t.Errorf("Unexpected node status, expected %v, got %v", NODE_UP, node.status)
	}

}

// tests that a node is marked as down if sending
// a message fails
func TestMessageSendingFailureCase(t *testing.T) {
	cluster := setupCluster()
	node := NewRemoteNode("127.0.0.2:9998", cluster)
	node.status = NODE_UP
	conn := &Connection{socket:&readTimeoutConn{}}
	conn.SetHandshakeCompleted()
	node.pool.Put(conn)

	request := &DiscoverPeersRequest{NodeId:node.GetId()}
	response, mtype, err := node.sendMessage(request)
	if err == nil {
		t.Fatal("Expected error, received nil")
	}
	if response != nil || mtype != 0 {
		t.Errorf("Expected nil message and response type, got: %T %v", response, mtype)
	}
	if node.status != NODE_DOWN {
		t.Errorf("Unexpected node status. Expected %v, got %v", NODE_DOWN, node.status)
	}

}

// tests that calling ExecuteRead sends a ReadMessage
func TestReadRequestSendsCorrectMessage(t *testing.T) {
	t.SkipNow()
}

// tests that calling ExecuteWrite sends a WriteMessage
func TestWriteRequestSendsCorrectMessage(t *testing.T) {
	t.SkipNow()
}
