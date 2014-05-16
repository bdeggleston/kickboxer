package topology

import (
	"fmt"
)

import (
	"launchpad.net/gocheck"
)

import (
	"node"
	"partitioner"
)

type RingTest struct {
	ring *Ring
}

var _ = gocheck.Suite(&RingTest{})

func (t *RingTest) SetUpTest(c *gocheck.C) {
	t.ring = NewRing()

	for i:=0; i<10; i++ {
		n := newMockNode(
			node.NewNodeId(),
			DatacenterID("DC5000"),
			partitioner.Token([]byte{0,0,byte(i),0}),
			fmt.Sprintf("N%v", i),
		)
		t.ring.AddNode(n)
	}
}

/************** GetNode tests **************/

func (t *RingTest) TestGetUnknownNode(c *gocheck.C) {
	n, err := t.ring.getNode(node.NewNodeId())
	c.Assert(n, gocheck.IsNil)
	c.Assert(err, gocheck.NotNil)
}

// tests that fetching a node with a valid node id
// returns the requested node
func (t *RingTest) TestGetExistingNode(c *gocheck.C) {
	nid := t.ring.tokenRing[4].GetId()
	n, err := t.ring.getNode(nid)
	c.Assert(err, gocheck.IsNil)
	c.Assert(n, gocheck.NotNil)
	c.Check(n.GetId(), gocheck.Equals, nid)
}

/************** AddNode tests **************/

// tests that a node is added to the cluster if
// the cluster has not seen it yet
func (t *RingTest) TestAddingNewNodeToRing(c *gocheck.C) {
	// sanity check
	c.Assert(len(t.ring.nodeMap), gocheck.Equals, 10)
	c.Assert(len(t.ring.tokenRing), gocheck.Equals, 10)

	// add a new node
	token := partitioner.Token([]byte{0,0,1,2,3,4,5,6,7,0,1,2,3,4,5,6})
	newNode := newMockNode(node.NewNodeId(), "DC1", token, "N2")
	err := t.ring.AddNode(newNode)
	c.Assert(err, gocheck.IsNil)

	c.Check(len(t.ring.nodeMap), gocheck.Equals, 11)
	c.Check(len(t.ring.tokenRing), gocheck.Equals, 11)
	c.Check(newNode.isStarted, gocheck.Equals, false)
	c.Check(len(newNode.requests), gocheck.Equals, 0)
}

// tests that nothing is changed if a node is already
// known by the cluster, and an error is returned
func (t *RingTest) TestAddingExistingNodeToRing(c *gocheck.C) {
	// sanity check
	c.Assert(len(t.ring.nodeMap), gocheck.Equals, 10)
	c.Assert(len(t.ring.tokenRing), gocheck.Equals, 10)

	err := t.ring.AddNode(t.ring.tokenRing[3])
	c.Assert(err, gocheck.NotNil)

	c.Check(len(t.ring.nodeMap), gocheck.Equals, 10)
	c.Check(len(t.ring.tokenRing), gocheck.Equals, 10)
}

/************** AllNodes tests **************/

func (t *RingTest) TestAllNodes(c *gocheck.C) {
	nodes := t.ring.AllNodes()
	c.Assert(len(t.ring.nodeMap), gocheck.Equals, len(nodes))

	for i:=0; i<len(nodes); i++ {
		expected := t.ring.tokenRing[i]
		actual := nodes[i]

		c.Assert(actual, gocheck.NotNil)
		c.Check(actual.GetId(), gocheck.Equals, expected.GetId())
	}
}

/************** RefreshRing tests **************/

func (t *RingTest) TestRingIsRefreshedAfterNodeAddition(c *gocheck.C) {
	ring := NewRing()

	n1 := newMockNode(
		node.NewNodeId(),
		"DC1",
		partitioner.Token([]byte{0,0,0,7}),
		fmt.Sprintf("N1"),
	)
	ring.AddNode(n1)

	n2 := newMockNode(
		node.NewNodeId(),
		"DC1",
		partitioner.Token([]byte{0,0,0,3}),
		"N2",
	)
	ring.AddNode(n2)

	c.Check(n2.GetId(), gocheck.Equals, ring.tokenRing[0].GetId())
	c.Check(n1.GetId(), gocheck.Equals, ring.tokenRing[1].GetId())

	n3 := newMockNode(
		node.NewNodeId(),
		"DC1",
		partitioner.Token([]byte{0,0,0,5}),
		"N3",
	)
	ring.AddNode(n3)

	c.Check(n2.GetId(), gocheck.Equals, ring.tokenRing[0].GetId())
	c.Check(n3.GetId(), gocheck.Equals, ring.tokenRing[1].GetId())
	c.Check(n1.GetId(), gocheck.Equals, ring.tokenRing[2].GetId())

	n4 := newMockNode(
		node.NewNodeId(),
		"DC1",
		partitioner.Token([]byte{0,0,1,0}),
		"N4",
	)
	ring.AddNode(n4)

	c.Check(n2.GetId(), gocheck.Equals, ring.tokenRing[0].GetId())
	c.Check(n3.GetId(), gocheck.Equals, ring.tokenRing[1].GetId())
	c.Check(n1.GetId(), gocheck.Equals, ring.tokenRing[2].GetId())
	c.Check(n4.GetId(), gocheck.Equals, ring.tokenRing[3].GetId())
}

// tests that the proper nodes are returned for the given keys
func (t *RingTest) TestKeyRouting(c *gocheck.C) {
	var token partitioner.Token
	var nodes []ClusterNode

	// test the upper bound
	token = partitioner.Token([]byte{0,0,9,5})
	nodes = t.ring.GetNodesForToken(token, 3)
	c.Assert(len(nodes), gocheck.Equals, 3)
	c.Check(t.ring.tokenRing[0].GetId(), gocheck.Equals, t.ring.tokenRing[0].GetId())
	c.Check(t.ring.tokenRing[1].GetId(), gocheck.Equals, t.ring.tokenRing[1].GetId())
	c.Check(t.ring.tokenRing[2].GetId(), gocheck.Equals, t.ring.tokenRing[2].GetId())

	// test the lower bound
	token = partitioner.Token([]byte{0,0,0,0})
	nodes = t.ring.GetNodesForToken(token, 3)
	c.Assert(len(nodes), gocheck.Equals, 3)
	c.Check(nodes[0].GetId(), gocheck.Equals, t.ring.tokenRing[0].GetId())
	c.Check(nodes[1].GetId(), gocheck.Equals, t.ring.tokenRing[1].GetId())
	c.Check(nodes[2].GetId(), gocheck.Equals, t.ring.tokenRing[2].GetId())

	// test token intersection
	token = partitioner.Token([]byte{0,0,4,0})
	nodes = t.ring.GetNodesForToken(token, 3)
	c.Assert(len(nodes), gocheck.Equals, 3)
	c.Check(nodes[0].GetId(), gocheck.Equals, t.ring.tokenRing[4].GetId())
	c.Check(nodes[1].GetId(), gocheck.Equals, t.ring.tokenRing[5].GetId())
	c.Check(nodes[2].GetId(), gocheck.Equals, t.ring.tokenRing[6].GetId())

	// test middle range
	token = partitioner.Token([]byte{0,0,4,5})
	nodes = t.ring.GetNodesForToken(token, 3)
	c.Assert(len(nodes), gocheck.Equals, 3)
	c.Check(nodes[0].GetId(), gocheck.Equals, t.ring.tokenRing[5].GetId())
	c.Check(nodes[1].GetId(), gocheck.Equals, t.ring.tokenRing[6].GetId())
	c.Check(nodes[2].GetId(), gocheck.Equals, t.ring.tokenRing[7].GetId())

	// test wrapping
	token = partitioner.Token([]byte{0,0,8,5})
	nodes = t.ring.GetNodesForToken(token, 3)
	c.Assert(len(nodes), gocheck.Equals, 3)
	c.Check(nodes[0].GetId(), gocheck.Equals, t.ring.tokenRing[9].GetId())
	c.Check(nodes[1].GetId(), gocheck.Equals, t.ring.tokenRing[0].GetId())
	c.Check(nodes[2].GetId(), gocheck.Equals, t.ring.tokenRing[1].GetId())
}

// TODO: this
// tests that the number of nodes returned matches the replication factor
func (t *RingTest) TestReplicationFactor(c *gocheck.C) {
	c.Skip("check number of nodes returned matches replication factor")

}
