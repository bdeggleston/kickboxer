package cluster

import (
	"fmt"
	"time"
	"testing"
)

import (
	"message"
	"node"
	"partitioner"
	"store"
	"topology"
)

type queryCall struct {
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
	requests []queryCall
	responses chan *mockQueryResponse

	// for the logging
	testPtr *testing.T
}

func newMockNode(id node.NodeId, dcid topology.DatacenterID, token partitioner.Token, name string) (*mockNode) {
	n := &mockNode{}
	n.id = id
	n.dcId = dcid
	n.token = token
	n.name = name
	n.status = topology.NODE_UP
	n.requests = make([]queryCall, 0, 5)
	n.responses = make(chan *mockQueryResponse, 100)
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

// executes a write instruction against the node's store
func (n *mockNode) ExecuteQuery(cmd string, key string, args []string, timestamp time.Time) (store.Value, error) {
	call := queryCall{cmd:cmd, key:key, args:args, timestamp:timestamp}
	n.requests = append(n.requests, call)

	n.log(fmt.Sprintf("Write Requested: %v, %v, %v, %v", cmd, key, args, timestamp))
	response := <- n.responses
	n.log(fmt.Sprintf("Read Response: %v", response.val))
	return response.val, response.err
}

func (n *mockNode) SendMessage(m message.Message) (message.Message, error) {
	panic("implement it if you need it")
}

// log to the test pointer, if it exists
func (n *mockNode) log(msg string) {
	if n.testPtr != nil {
		n.testPtr.Logf("Node %v: %v\n", n.name, msg)
	}
}

func (n *mockNode) addResponse(val store.Value, err error) {
	n.log(fmt.Sprintf("Recieved write return val: %v, %v", val, err))
	n.responses <- &mockQueryResponse{val:val, err:err}
}
