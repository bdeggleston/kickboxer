package topology

import (
	"fmt"
	"testing"
)

import (
	"launchpad.net/gocheck"
)

import (
	"node"
	"partitioner"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	gocheck.TestingT(t)
}

type TopologyTest struct {
	tp *Topology
	localNID node.NodeId
	localDCID DatacenterID
	numDCs int
	numNodes int
	replicationFactor int
}

var _ = gocheck.Suite(&TopologyTest{})

func (t *TopologyTest) SetUpSuite(c *gocheck.C) {
	t.numDCs = 3
	t.numNodes = 10
	t.replicationFactor = 3
}

func (t *TopologyTest) SetUpTest(c *gocheck.C) {
	t.localNID = node.NewNodeId()
	t.localDCID = DatacenterID("DC1")
	t.tp = NewTopology(t.localNID, t.localDCID, partitioner.NewMD5Partitioner(), 3)

	for i:=0; i<t.numDCs; i++ {
		dcNum := i+1
		var dcid DatacenterID
		if i == 0 {
			dcid = t.localDCID
		} else {
			dcid = DatacenterID(fmt.Sprintf("DC%v", dcNum))
		}

		for j:=0; j<t.numNodes; j++ {
			var nid node.NodeId
			if i == 0 && j == 0 {
				nid = t.localNID
			} else {
				nid = node.NewNodeId()
			}
			n := newMockNode(
				nid,
				dcid,
				partitioner.Token([]byte{0,0,byte(j),0}),
				fmt.Sprintf("N%v", j),
			)
			t.tp.AddNode(n)
		}
	}
	c.Assert(len(t.tp.rings), gocheck.Equals, 3)
}

// tests add node behavior
func (t *TopologyTest) TestAddNode(c *gocheck.C) {
	for i, dcid := range []DatacenterID{"DC1", "DC2", "DC3"} {
		dcNum := i + 1
		ring, exists := t.tp.rings[dcid]
		c.Check(exists, gocheck.Equals, true)
		nodes := ring.AllNodes()
		c.Check(len(nodes), gocheck.Equals, 10)
		n := nodes[0]
		c.Check(n.GetDatacenterId(), gocheck.Equals, DatacenterID(fmt.Sprintf("DC%v", dcNum)))
	}
}

func (t *TopologyTest) TestGetRing(c *gocheck.C) {
	ring, err := t.tp.GetRing("DC1")
	c.Assert(err, gocheck.IsNil)
	c.Assert(ring, gocheck.NotNil)

	ring, err = t.tp.GetRing("DC5")
	c.Assert(err, gocheck.NotNil)
	c.Assert(ring, gocheck.IsNil)
}

func (t *TopologyTest) TestGetNodesForToken(c *gocheck.C) {
	var token partitioner.Token

	token = partitioner.Token([]byte{0,0,4,5})
	nodes := t.tp.GetNodesForToken(token)

	c.Assert(len(nodes), gocheck.Equals, 3)
	for dcid, replicas := range nodes {
		c.Check(replicas[0].GetId(), gocheck.Equals, t.tp.rings[dcid].tokenRing[5].GetId())
		c.Check(replicas[1].GetId(), gocheck.Equals, t.tp.rings[dcid].tokenRing[6].GetId())
		c.Check(replicas[2].GetId(), gocheck.Equals, t.tp.rings[dcid].tokenRing[7].GetId())
	}
}

func (t *TopologyTest) TestGetLocalNodesForToken(c *gocheck.C) {
	token := partitioner.Token([]byte{0,0,4,5})
	nodes := t.tp.GetLocalNodesForToken(token)

	c.Check(nodes[0].GetId(), gocheck.Equals, t.tp.rings[t.localDCID].tokenRing[5].GetId())
	c.Check(nodes[1].GetId(), gocheck.Equals, t.tp.rings[t.localDCID].tokenRing[6].GetId())
	c.Check(nodes[2].GetId(), gocheck.Equals, t.tp.rings[t.localDCID].tokenRing[7].GetId())
}

func (t *TopologyTest) TestTokenIsLocallyReplicated(c *gocheck.C) {
	// local token in 0000, 90, 80, & 70 should be replicated
	for i:=0; i<t.numNodes; i++ {
		tk := partitioner.Token([]byte{0,0,byte(i),0})
		shouldReplicate := i > (t.numNodes - t.replicationFactor) || i == 0
		c.Check(t.tp.TokenLocallyReplicated(tk), gocheck.Equals, shouldReplicate, gocheck.Commentf("%v: %v", i, tk))
	}
}
