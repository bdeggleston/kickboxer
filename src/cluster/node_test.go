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
// a connection to the remote peer server
func TestStartingConnectsToPeer(t *testing.T) {

}

// tests that sending and receiving messages works as
// expected
func TestMessageSendingSuccessCase(t *testing.T) {

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
