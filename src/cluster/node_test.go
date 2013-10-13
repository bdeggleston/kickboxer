/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/9/13
 * Time: 7:27 AM
 * To change this template use File | Settings | File Templates.
 */
package cluster

import (
	"testing"
	"time"
)

type readCall struct {
	cmd string
	key string
	args []string
}

type writeCall struct {
	cmd string
	key string
	args []string
	timestamp time.Time
}

type mockNode struct {
	name string
	token Token
	id NodeId
	status NodeStatus

	isStarted bool
	reads []readCall
	writes []writeCall
}

func newMockNode(id NodeId, token Token, name string) (*mockNode) {
	n := &mockNode{id:id, token:token, name:name}
	n.status = NODE_UP
	n.reads = make([]readCall, 0, 5)
	n.writes = make([]writeCall, 0, 5)
	return n
}

func (n *mockNode) Name() string { return n.name }

func (n *mockNode) GetToken() Token { return n.token }

func (n *mockNode) GetStatus() NodeStatus { return n.status }

func (n *mockNode) GetId() NodeId { return n.id }

func (n *mockNode) Start() error {
	n.isStarted = true
	return nil
}

func (n *mockNode) Stop() error {
	n.isStarted = false
	return nil
}

func (n *mockNode) IsStarted() bool {
	return n.isStarted
}

func (n *mockNode) ExecuteRead(cmd string, key string, args []string) {
	call := readCall{cmd:cmd, key:key, args:args}
	n.reads = append(n.reads, call)
}

// executes a write instruction against the node's store
func (n *mockNode) ExecuteWrite(cmd string, key string, args []string, timestamp time.Time) {
	call := writeCall{cmd:cmd, key:key, args:args, timestamp:timestamp}
	n.writes = append(n.writes, call)
}


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
	equalityCheck(t, "pre start conn status", false, conn.completedHandshake)
	equalityCheck(t, "pre start name", "", node.name)
	equalityCheck(t, "pre start id", NodeId(""), node.id)
	equalityCheck(t, "pre start status", NODE_INITIALIZING, node.status)
	sliceEqualityCheck(t, "pre start token", nil, node.token)

	// start the node
	err := node.Start()
	if err != nil {
		t.Fatalf("Unexpected error of type %T: %v", err, err)
	}

	// check that values were saved properly
	equalityCheck(t, "post start conn status", true, conn.completedHandshake)
	equalityCheck(t, "post start name", response.Name, node.name)
	equalityCheck(t, "post start id", response.NodeId, node.id)
	equalityCheck(t, "post start status", NODE_UP, node.status)
	sliceEqualityCheck(t, "post start token", response.Token, node.token)


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

}

// tests that the handshake happens when
// getting a newly constructed connection from the pool
func TestGetConnectionPerformsHandshake(t *testing.T) {

}

// tests that calling ExecuteRead sends a ReadMessage
func TestReadRequestSendsCorrectMessage(t *testing.T) {

}

// tests that calling ExecuteWrite sends a WriteMessage
func TestWriteRequestSendsCorrectMessage(t *testing.T) {

}
