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

func (s *ConsensusMessageTest) TestAcceptRequest(c *gocheck.C) {
	var err error
	buf := &bytes.Buffer{}
	src := &AcceptRequest{
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

func (s *ConsensusMessageTest) TestAcceptResponse(c *gocheck.C) {
	var err error
	buf := &bytes.Buffer{}
	src := &AcceptResponse{
		Accepted: true,
		MaxBallot: uint32(6),
	}

	err = message.WriteMessage(buf, src)
	c.Assert(err, gocheck.IsNil)

	dst, err := message.ReadMessage(buf)
	c.Assert(err, gocheck.IsNil)
	c.Check(dst, gocheck.DeepEquals, src)
}

func (s *ConsensusMessageTest) TestCommitRequest(c *gocheck.C) {
	var err error
	buf := &bytes.Buffer{}
	src := &CommitRequest{
		Instance: makeInstance(node.NewNodeId(), makeDependencies(3)),
	}

	err = message.WriteMessage(buf, src)
	c.Assert(err, gocheck.IsNil)

	dst, err := message.ReadMessage(buf)
	c.Assert(err, gocheck.IsNil)
	c.Check(dst, gocheck.DeepEquals, src)
}

func (s *ConsensusMessageTest) TestCommitResponse(c *gocheck.C) {
	var err error
	buf := &bytes.Buffer{}
	src := &CommitResponse{}

	err = message.WriteMessage(buf, src)
	c.Assert(err, gocheck.IsNil)

	dst, err := message.ReadMessage(buf)
	c.Assert(err, gocheck.IsNil)
	c.Check(dst, gocheck.DeepEquals, src)
}

func (s *ConsensusMessageTest) TestPrepareRequest(c *gocheck.C) {
	var err error
	buf := &bytes.Buffer{}
	src := &PrepareRequest{
		Ballot: uint32(52),
		InstanceID: NewInstanceID(),
	}

	err = message.WriteMessage(buf, src)
	c.Assert(err, gocheck.IsNil)

	dst, err := message.ReadMessage(buf)
	c.Assert(err, gocheck.IsNil)
	c.Check(dst, gocheck.DeepEquals, src)
}

func (s *ConsensusMessageTest) TestPrepareResponse(c *gocheck.C) {
	var err error
	buf := &bytes.Buffer{}
	src := &PrepareResponse{
		Accepted: true,
		Instance: makeInstance(node.NewNodeId(), makeDependencies(3)),
	}

	err = message.WriteMessage(buf, src)
	c.Assert(err, gocheck.IsNil)

	dst, err := message.ReadMessage(buf)
	c.Assert(err, gocheck.IsNil)
	c.Check(dst, gocheck.DeepEquals, src)
}

func (s *ConsensusMessageTest) TestPrepareResponseNoInstance(c *gocheck.C) {
	var err error
	buf := &bytes.Buffer{}
	src := &PrepareResponse{
		Accepted: true,
		Instance: nil,
	}

	err = message.WriteMessage(buf, src)
	c.Assert(err, gocheck.IsNil)

	dst, err := message.ReadMessage(buf)
	c.Assert(err, gocheck.IsNil)
	c.Check(dst, gocheck.DeepEquals, src)
}

func (s *ConsensusMessageTest) TestPrepareSuccessorRequest(c *gocheck.C) {
	var err error
	buf := &bytes.Buffer{}
	src := &PrepareSuccessorRequest{
		InstanceID: NewInstanceID(),
	}

	err = message.WriteMessage(buf, src)
	c.Assert(err, gocheck.IsNil)

	dst, err := message.ReadMessage(buf)
	c.Assert(err, gocheck.IsNil)
	c.Check(dst, gocheck.DeepEquals, src)
}

func (s *ConsensusMessageTest) TestPrepareSuccessorResponse(c *gocheck.C) {
	var err error
	buf := &bytes.Buffer{}
	src := &PrepareSuccessorResponse{
		Instance: makeInstance(node.NewNodeId(), makeDependencies(3)),
	}

	err = message.WriteMessage(buf, src)
	c.Assert(err, gocheck.IsNil)

	dst, err := message.ReadMessage(buf)
	c.Assert(err, gocheck.IsNil)
	c.Check(dst, gocheck.DeepEquals, src)
}

func (s *ConsensusMessageTest) TestPrepareSuccessorResponseNoInstance(c *gocheck.C) {
	var err error
	buf := &bytes.Buffer{}
	src := &PrepareSuccessorResponse{
		Instance: nil,
	}

	err = message.WriteMessage(buf, src)
	c.Assert(err, gocheck.IsNil)

	dst, err := message.ReadMessage(buf)
	c.Assert(err, gocheck.IsNil)
	c.Check(dst, gocheck.DeepEquals, src)
}

func (s *ConsensusMessageTest) TestInstanceRequest(c *gocheck.C) {
	var err error
	buf := &bytes.Buffer{}
	src := &InstanceRequest{
		InstanceIDs: makeDependencies(4),
	}

	err = message.WriteMessage(buf, src)
	c.Assert(err, gocheck.IsNil)

	dst, err := message.ReadMessage(buf)
	c.Assert(err, gocheck.IsNil)
	c.Check(dst, gocheck.DeepEquals, src)
}

func (s *ConsensusMessageTest) TestInstanceResponse(c *gocheck.C) {
	var err error
	buf := &bytes.Buffer{}
	src := &InstanceResponse{
		Instances: []*Instance{
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
