package topology

import (
	"fmt"
)

import (
	"launchpad.net/gocheck"
)

import (
	"partitioner"
)

type DatacenterTest struct {
	dc *DatacenterContainer
}

var _ = gocheck.Suite(&DatacenterTest{})

func (t *DatacenterTest) SetUpTest(c *gocheck.C) {
	t.dc = setupDC(3, 10)
	c.Assert(len(t.dc.rings), gocheck.Equals, 3)
}

// tests add node behavior
func (t *DatacenterTest) TestAddNode(c *gocheck.C) {
	for i, dcid := range []DatacenterID{"DC1", "DC2", "DC3"} {
		dcNum := i + 1
		ring, exists := t.dc.rings[dcid]
		c.Check(exists, gocheck.Equals, true)
		nodes := ring.AllNodes()
		c.Check(len(nodes), gocheck.Equals, 10)
		node := nodes[0]
		c.Check(node.GetDatacenterId(), gocheck.Equals, DatacenterID(fmt.Sprintf("DC%v", dcNum)))
	}
}

func (t *DatacenterTest) TestGetRing(c *gocheck.C) {
	ring, err := t.dc.GetRing("DC1")
	c.Assert(err, gocheck.IsNil)
	c.Assert(ring, gocheck.NotNil)

	ring, err = t.dc.GetRing("DC5")
	c.Assert(err, gocheck.NotNil)
	c.Assert(ring, gocheck.IsNil)
}

func (t *DatacenterTest) TestGetNodesForToken(c *gocheck.C) {
	var token partitioner.Token

	token = partitioner.Token([]byte{0,0,4,5})
	nodes := t.dc.GetNodesForToken(token, 3)

	c.Assert(len(nodes), gocheck.Equals, 3)
	for dcid, replicas := range nodes {
		c.Check(replicas[0].GetId(), gocheck.Equals, t.dc.rings[dcid].tokenRing[5].GetId())
		c.Check(replicas[1].GetId(), gocheck.Equals, t.dc.rings[dcid].tokenRing[6].GetId())
		c.Check(replicas[2].GetId(), gocheck.Equals, t.dc.rings[dcid].tokenRing[7].GetId())
	}
}

