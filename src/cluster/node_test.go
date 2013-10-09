/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/9/13
 * Time: 7:27 AM
 * To change this template use File | Settings | File Templates.
 */
package cluster

import (
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

	is_started bool
	reads []readCall
	writes []writeCall
}

func newMockNode(id NodeId, token Token, name string) (*mockNode) {
	n := &mockNode{id:id, token:token, name:name}
	n.reads = make([]readCall, 0, 5)
	n.writes = make([]writeCall, 0, 5)
	return n
}

func (n *mockNode) Name() string { return n.name }

func (n *mockNode) GetToken() Token { return n.token }

func (n *mockNode) GetId() NodeId { return n.id }

func (n *mockNode) Start() {
	n.is_started = true
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
