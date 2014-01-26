package consensus

import (
	"bytes"
)

import (
	"launchpad.net/gocheck"
)

import (
	"message"
	"node"
)

type ConsensusMessageTest struct { }

var _ = gocheck.Suite(&ConsensusMessageTest{})

func (s *ConsensusMessageTest) TestPreAcceptRequest(c *gocheck.C) {
	var err error
	buf := &bytes.Buffer{}
	src := &PreAcceptRequest{
		Scope: "abc",
		Instance: makeInstance(node.NewNodeId(), makeDependencies(3)),
	}

	err = message.WriteMessage(buf, src)
	c.Assert(err, gocheck.IsNil)

	dst, err := message.ReadMessage(buf)
	c.Assert(err, gocheck.IsNil)
	c.Check(dst, gocheck.DeepEquals, src)
}

func (s *ConsensusMessageTest) TestPreAcceptResponse(c *gocheck.C) {
	var err error
	buf := &bytes.Buffer{}
	src := &PreAcceptResponse{
		Accepted: true,
		MaxBallot: uint32(6),
		Instance: makeInstance(node.NewNodeId(), makeDependencies(3)),
		MissingInstances: []*Instance{
			makeInstance(node.NewNodeId(), makeDependencies(3)),
			makeInstance(node.NewNodeId(), makeDependencies(3)),
		},
	}

	err = message.WriteMessage(buf, src)
	c.Assert(err, gocheck.IsNil)

	dst, err := message.ReadMessage(buf)
	c.Assert(err, gocheck.IsNil)
	c.Check(dst, gocheck.DeepEquals, src)
}
