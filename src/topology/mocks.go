package topology

import (
	"time"
)


import (
	"message"
	"node"
	"partitioner"
	"store"
)

type mockNode struct {
	id node.NodeId
	dcID DatacenterID
	token partitioner.Token
	name string
	status NodeStatus
	started bool
}

var _ = Node(&mockNode{})

func newMockNode(id node.NodeId, dcid DatacenterID, token partitioner.Token, name string) (*mockNode) {
	n := &mockNode{}
	n.id = id
	n.dcID = dcid
	n.token = token
	n.name = name
	n.status = NODE_UP
	return n
}

func (n *mockNode) GetId() node.NodeId { return n.id }
func (n *mockNode) Name() string { return n.name }
func (n *mockNode) GetAddr() string { return "" }
func (n *mockNode) GetToken() partitioner.Token { return n.token }
func (n *mockNode) GetDatacenterId() DatacenterID { return n.dcID }
func (n *mockNode) GetStatus() NodeStatus { return n.status }
func (n *mockNode) IsStarted() bool { return n.started }

func (n *mockNode) Start() error {
	n.started = true
	return nil
}

func (n *mockNode) Stop() error {
	n.started = false;
	return nil
}

func (n *mockNode) ExecuteQuery(cmd string, key string, args []string, timestamp time.Time) (store.Value, error) {
	panic("not implemented")
}
func (n *mockNode) SendMessage(message.Message) (message.Message, error) {
	panic("not implemented")
}
