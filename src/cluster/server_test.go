/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/7/13
 * Time: 9:00 PM
 * To change this template use File | Settings | File Templates.
 */
package cluster

import (
	"bytes"
	"strings"
	"testing"
)


// tests that a properly formed connection request
// gets a connection accepted
func TestServerConnectionSuccessCase(t *testing.T) {
	conn := newBiConn(2,1)

	// write input messages
	connectMessage := &ConnectionRequest{PeerData{
		NodeId:NewNodeId(),
		Addr:"127.0.0.1:9999",
		Name:"Test Node",
		Token:Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
	}}
	if err := WriteMessage(conn.input[0], connectMessage); err != nil {
		t.Fatalf("Unexpected error writing connection message: %v", err)
	}
	closeMessage := &closeConnection{}
	if err := WriteMessage(conn.input[1], closeMessage); err != nil {
		t.Fatalf("Unexpected error writing close message: %v", err)
	}

	// create cluster and peer server
	token := Token([]byte{4,5,6,7,0,1,2,3,4,5,6,7,0,1,2,3})
	cluster, err := NewCluster("127.0.0.1:9999", "TestCluster", token, NewNodeId(), 3)
	if err != nil {
		t.Fatalf("Unexpected error creating mock cluster: %v", err)
	}
	server := &PeerServer{cluster:cluster}
	err = server.handleConnection(conn)
	if err != nil {
		t.Fatalf("Unexpected error from connection method: %v", err)
	}

	// read output messages
	rawAcceptMessage, msgType, err := ReadMessage(conn.output[0])
	if err != nil {
		t.Errorf("Unexpected error reading acceptance message")
	}

	// verify output
	if msgType != CONNECTION_ACCEPTED_RESPONSE {
		t.Errorf("Expected CONNECTION_ACCEPTED_RESPONSE, got: %v", msgType)
	}
	acceptMessage, ok := rawAcceptMessage.(*ConnectionAcceptedResponse)
	if !ok {
		t.Errorf("Expected ConnectionAcceptedResponse, got: %T", rawAcceptMessage)
	}
	if acceptMessage.Name != cluster.GetName() {
		t.Errorf("Unexpected Name value. Expected %v, got %v", cluster.GetName(), acceptMessage.Name)
	}
	if acceptMessage.NodeId != cluster.GetNodeId() {
		t.Errorf("Unexpected NodeId value. Expected %v, got %v", cluster.GetNodeId(), acceptMessage.NodeId)
	}
	if !bytes.Equal(acceptMessage.Token, cluster.GetToken()) {
		t.Errorf("Unexpected Token value. Expected %v, got %v", cluster.GetToken(), acceptMessage.Token)
	}
}

// tests sending a message other than connection request
// as a first message results in a closed connection
func TestServerConnectionFailure(t *testing.T) {
	conn := newBiConn(1,1)

	// write input messages
	connectMessage := &ReadRequest{Cmd:"GET", Key:"A", Args:[]string{"B"}}
	if err := WriteMessage(conn.input[0], connectMessage); err != nil {
		t.Fatalf("Unexpected error writing connection message: %v", err)
	}

	// create cluster and peer server
	token := Token([]byte{4,5,6,7,0,1,2,3,4,5,6,7,0,1,2,3})
	cluster, err := NewCluster("127.0.0.1:9999", "TestCluster", token, NewNodeId(), 3)
	if err != nil {
		t.Fatalf("Unexpected error creating mock cluster: %v", err)
	}
	server := &PeerServer{cluster:cluster}
	err = server.handleConnection(conn)
	if err == nil {
		t.Fatalf("Unexpected nil error from handleConnection")
	}

	// read output messages
	rawAcceptMessage, msgType, err := ReadMessage(conn.output[0])
	if err != nil {
		t.Errorf("Unexpected error reading acceptance message")
	}

	// verify output
	if msgType != CONNECTION_REFUSED_RESPONSE {
		t.Errorf("Expected CONNECTION_ACCEPTED_RESPONSE, got: %v", msgType)
	}
	refusalMessage, ok := rawAcceptMessage.(*ConnectionRefusedResponse)
	if !ok {
		t.Errorf("Expected ConnectionRefusedResponse, got: %T", rawAcceptMessage)
	}
	if !strings.Contains(refusalMessage.Reason, "ConnectionRequest expected") {
		t.Errorf("Unexpected refusal reason: %v", refusalMessage.Reason)
	}
}

// tests that a node is registered with the cluster
// when it connects for the first time
func TestServerNodeRegistrationOnConnection(t *testing.T) {
	conn := newBiConn(2,1)

	// write input messages
	connectMessage := &ConnectionRequest{PeerData{
		NodeId:NewNodeId(),
		Addr:"127.0.0.1:9999",
		Name:"Test Node",
		Token:Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
	}}
	if err := WriteMessage(conn.input[0], connectMessage); err != nil {
		t.Fatalf("Unexpected error writing connection message: %v", err)
	}
	closeMessage := &closeConnection{}
	if err := WriteMessage(conn.input[1], closeMessage); err != nil {
		t.Fatalf("Unexpected error writing close message: %v", err)
	}

	// create cluster and peer server
	token := Token([]byte{4,5,6,7,0,1,2,3,4,5,6,7,0,1,2,3})
	cluster, err := NewCluster("127.0.0.1:9999", "TestCluster", token, NewNodeId(), 3)
	if err != nil {
		t.Fatalf("Unexpected error creating mock cluster: %v", err)
	}
	// sanity check
	if _, err := cluster.getNode(connectMessage.NodeId); err == nil {
		t.Fatalf("Unexpected nil error getting new node")
	}

	server := &PeerServer{cluster:cluster}
	err = server.handleConnection(conn)
	if err != nil {
		t.Fatalf("Unexpected error from connection method: %v", err)
	}
	if _, err := cluster.getNode(connectMessage.NodeId); err != nil {
		t.Fatalf("Unexpected error getNode: %v", err)
	}
}

