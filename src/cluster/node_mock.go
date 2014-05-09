package cluster

import (
	"fmt"
	"time"
	"testing"
)

import (
	"node"
	"store"
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

type mockQueryResponse struct {
	val store.Value
	err error
}

type mockNode struct {
	baseNode
	isStarted bool
	reads []readCall
	writes []writeCall
	readResponses chan *mockQueryResponse
	writeResponses chan *mockQueryResponse

	// for the logging
	testPtr *testing.T
}

func newMockNode(id node.NodeId, dcid DatacenterId, token Token, name string) (*mockNode) {
	n := &mockNode{}
	n.id = id
	n.dcId = dcid
	n.token = token
	n.name = name
	n.status = NODE_UP
	n.reads = make([]readCall, 0, 5)
	n.writes = make([]writeCall, 0, 5)
	n.readResponses = make(chan *mockQueryResponse, 100)
	n.writeResponses = make(chan *mockQueryResponse, 100)
	return n
}

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

func (n *mockNode) ExecuteRead(cmd string, key string, args []string) (store.Value, error) {
	call := readCall{cmd:cmd, key:key, args:args}
	n.reads = append(n.reads, call)

	n.log(fmt.Sprintf("Read Requested: %v, %v, %v", cmd, key, args))
	response := <- n.readResponses
	n.log(fmt.Sprintf("Read Response: %v", response.val))
	return response.val, response.err
}

// executes a write instruction against the node's store
func (n *mockNode) ExecuteWrite(cmd string, key string, args []string, timestamp time.Time) (store.Value, error) {
	call := writeCall{cmd:cmd, key:key, args:args, timestamp:timestamp}
	n.writes = append(n.writes, call)

	n.log(fmt.Sprintf("Write Requested: %v, %v, %v, %v", cmd, key, args, timestamp))
	response := <- n.writeResponses
	n.log(fmt.Sprintf("Read Response: %v", response.val))
	return response.val, response.err
}

// log to the test pointer, if it exists
func (n *mockNode) log(msg string) {
	if n.testPtr != nil {
		n.testPtr.Logf("Node %v: %v\n", n.name, msg)
	}
}

func (n *mockNode) addReadResponse(val store.Value, err error) {
	n.log(fmt.Sprintf("Recieved read return val: %v, %v", val, err))
	n.readResponses <- &mockQueryResponse{val:val, err:err}
}

func (n *mockNode) addWriteResponse(val store.Value, err error) {
	n.log(fmt.Sprintf("Recieved write return val: %v, %v", val, err))
	n.writeResponses <- &mockQueryResponse{val:val, err:err}
}
