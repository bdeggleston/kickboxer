package cluster

import (
	"store"
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
}

func newMockNode(id NodeId, dcid DatacenterId, token Token, name string) (*mockNode) {
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

	response := <- n.readResponses
	return response.val, response.err
}

// executes a write instruction against the node's store
func (n *mockNode) ExecuteWrite(cmd string, key string, args []string, timestamp time.Time) (store.Value, error) {
	call := writeCall{cmd:cmd, key:key, args:args, timestamp:timestamp}
	n.writes = append(n.writes, call)

	response := <- n.writeResponses
	return response.val, response.err
}

