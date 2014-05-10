package cluster

import (
	"testing"
)

import (
	"message"
	"node"
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
		NodeId:node.NewNodeId(),
		Name:"Ghost",
		Token:Token([]byte{0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,3}),
	}
	message.WriteMessage(sock.input[0], response)

	cluster := setupCluster()
	n := NewRemoteNode("127.0.0.2:9998", cluster)
	conn := &Connection{socket:sock}
	n.pool.Put(conn)

	// pre startup sanity check
	testing_helpers.AssertEqual(t, "pre start conn status", false, conn.completedHandshake)
	testing_helpers.AssertEqual(t, "pre start name", "", n.name)
	testing_helpers.AssertEqual(t, "pre start id", node.NodeId{}, n.id)
	testing_helpers.AssertEqual(t, "pre start status", NODE_INITIALIZING, n.status)
	testing_helpers.AssertSliceEqual(t, "pre start token", nil, n.token)

	// start the node
	err := n.Start()
	if err != nil {
		t.Fatalf("Unexpected error of type %T: %v", err, err)
	}

	// check that values were saved properly
	testing_helpers.AssertEqual(t, "post start conn status", true, conn.completedHandshake)
	testing_helpers.AssertEqual(t, "post start name", response.Name, n.name)
	testing_helpers.AssertEqual(t, "post start id", response.NodeId, n.id)
	testing_helpers.AssertEqual(t, "post start status", NODE_UP, n.status)
	testing_helpers.AssertSliceEqual(t, "post start token", response.Token, n.token)


}

// tests that sending and receiving messages works as
// expected
func TestMessageSendingSuccessCase(t *testing.T) {
	sock := newBiConn(1, 1)
	expected := &DiscoverPeerResponse{Peers:[]*PeerData{}}

	message.WriteMessage(sock.input[0], expected)

	cluster := setupCluster()
	n := NewRemoteNode("127.0.0.2:9998", cluster)
	n.status = NODE_UP
	conn := &Connection{socket:sock}
	conn.SetHandshakeCompleted()
	n.pool.Put(conn)

	request := &DiscoverPeersRequest{NodeId:n.GetId()}
	rawResponse, err := n.SendMessage(request)
	if err != nil {
		t.Fatalf("Unexpected error of type [%T]: %v", err, err)
	}
	response, ok := rawResponse.(*DiscoverPeerResponse)
	if !ok {
		t.Fatalf("Unexpected message type. Expected DiscoverPeerResponse, got %T", rawResponse)
	}
	if len(response.Peers) != 0 {
		t.Errorf("Unexpected response data length. Expected 0, got %v", len(response.Peers))
	}

	if n.status != NODE_UP {
		t.Errorf("Unexpected node status, expected %v, got %v", NODE_UP, n.status)
	}

}

// tests that a node is marked as down if sending
// a message fails
func TestMessageSendingFailureCase(t *testing.T) {
	cluster := setupCluster()
	n := NewRemoteNode("127.0.0.2:9998", cluster)
	n.status = NODE_UP
	conn := &Connection{socket:&readTimeoutConn{}}
	conn.SetHandshakeCompleted()
	n.pool.Put(conn)

	request := &DiscoverPeersRequest{NodeId:n.GetId()}
	response, err := n.SendMessage(request)
	if err == nil {
		t.Fatal("Expected error, received nil")
	}
	if response != nil {
		t.Errorf("Expected nil message and response type, got: %T", response)
	}
	if n.status != NODE_DOWN {
		t.Errorf("Unexpected node status. Expected %v, got %v", NODE_DOWN, n.status)
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
