package consensus

import (
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


