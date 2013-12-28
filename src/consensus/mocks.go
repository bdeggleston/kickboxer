package consensus

import (
	"time"
)


import (
	"message"
	"node"
	"store"
)

type mockCluster struct {
	id node.NodeId
}

func newMockCluster() *mockCluster {
	return &mockCluster{
		id: node.NewNodeId(),
	}
}

func (c *mockCluster) GetID() node.NodeId { return c.id }
func (c *mockCluster) GetStore() store.Store { return nil }


type mockNode struct {
	id node.NodeId

	// tracks the queries executed
	// against this node
	queries []*store.Instruction

	cluster *mockCluster
	manager *Manager
}

func newMockNode() {
	cluster := newMockCluster()
	return &mockNode{
		id: cluster.GetID(),
		queries: []*store.Instruction{},
		cluster:cluster,
		manager:NewManager(cluster),
	}
}

func (n *mockNode) GetId() node.NodeId { return n.id }
func (n *mockNode) ExecuteQuery(cmd string, key string, args []string, timestamp time.Time) (store.Value, error) {
	store.NewInstruction(cmd, key, args, timestamp)
	return nil, nil
}

func (n *mockNode) SendMessage(msg message.Message) (message.Message, error) {
	return n.manager.HandleMessage(msg)
}
