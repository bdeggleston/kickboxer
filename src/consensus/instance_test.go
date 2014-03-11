package consensus

import (
	"bufio"
	"bytes"
	"time"
)

import (
	"launchpad.net/gocheck"
)

import (
	"node"
	"store"
)

type InstanceSetTest struct { }

var _ = gocheck.Suite(&InstanceSetTest{})

func (s *InstanceSetTest) TestSetEqual(c *gocheck.C) {
	ids := []InstanceID{NewInstanceID(), NewInstanceID()}
	set0 := NewInstanceIDSet(ids)
	set1 := NewInstanceIDSet(ids)
	c.Assert(set0.Equal(set1), gocheck.Equals, true)
}

func (s *InstanceSetTest) TestSetNotEqual(c *gocheck.C) {
	ids := []InstanceID{NewInstanceID(), NewInstanceID()}
	set0 := NewInstanceIDSet(ids)
	ids = append(ids, NewInstanceID())
	set1 := NewInstanceIDSet(ids)
	c.Assert(set0.Equal(set1), gocheck.Equals, false)
	c.Assert(set1.Equal(set0), gocheck.Equals, false)
}

func (s *InstanceSetTest) TestSetUnion(c *gocheck.C) {
	ids := []InstanceID{NewInstanceID(), NewInstanceID(), NewInstanceID()}
	set0 := NewInstanceIDSet(ids[:2])
	set1 := NewInstanceIDSet(ids[1:])

	c.Assert(len(set0), gocheck.Equals, 2)
	c.Assert(len(set1), gocheck.Equals, 2)

	set2 := set0.Union(set1)
	c.Assert(len(set2), gocheck.Equals, 3)
}

type InstanceSerializationTest struct { }

var _ = gocheck.Suite(&InstanceSerializationTest{})

func (s *InstanceSerializationTest) TestSerialization(c *gocheck.C) {
	var err error
	buf := &bytes.Buffer{}
	src := &Instance{
		InstanceID: NewInstanceID(),
		LeaderID: node.NewNodeId(),
		Successors: []node.NodeId{node.NewNodeId(), node.NewNodeId(), node.NewNodeId()},
		Commands: []*store.Instruction{
			store.NewInstruction("set", "a", []string{"b", "c"}, time.Now()),
			store.NewInstruction("get", "d", []string{"e", "f"}, time.Now()),
		},
		Dependencies: []InstanceID{NewInstanceID(), NewInstanceID()},
		Sequence: uint64(11),
		Status: INSTANCE_ACCEPTED,
		MaxBallot: uint32(500),
		Noop: true,
		commitTimeout: time.Now(),
		executeTimeout: time.Now(),
		DependencyMatch: true,
	}
	writer := bufio.NewWriter(buf)
	err = instanceSerialize(src, writer)
	c.Assert(err, gocheck.IsNil)
	err = writer.Flush()
	c.Assert(err, gocheck.IsNil)

	reader := bufio.NewReader(buf)
	dst, err := instanceDeserialize(reader)
	c.Assert(err, gocheck.IsNil)
	c.Assert(dst, gocheck.DeepEquals, src)
}
