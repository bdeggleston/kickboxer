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

type InstanceMapTest struct { }

var _ = gocheck.Suite(&InstanceMapTest{})

func (s *InstanceMapTest) TestGetAndSet(c *gocheck.C) {
	imap := NewInstanceMap()
	instance1 := makeInstance(node.NewNodeId(), []InstanceID{})
	instance2 := makeInstance(node.NewNodeId(), []InstanceID{})
	instance3 := makeInstance(node.NewNodeId(), []InstanceID{})
	instance4 := makeInstance(node.NewNodeId(), []InstanceID{})
	instance5 := makeInstance(node.NewNodeId(), []InstanceID{})

	c.Assert(imap.Get(instance1.InstanceID), gocheck.IsNil)
	c.Assert(imap.Get(instance2.InstanceID), gocheck.IsNil)
	c.Assert(imap.Get(instance3.InstanceID), gocheck.IsNil)
	c.Assert(imap.Get(instance4.InstanceID), gocheck.IsNil)
	c.Assert(imap.Get(instance5.InstanceID), gocheck.IsNil)

	imap.Add(instance1)
	c.Assert(imap.Get(instance1.InstanceID), gocheck.NotNil)
	c.Assert(imap.Get(instance2.InstanceID), gocheck.IsNil)
	c.Assert(imap.Get(instance3.InstanceID), gocheck.IsNil)
	c.Assert(imap.Get(instance4.InstanceID), gocheck.IsNil)
	c.Assert(imap.Get(instance5.InstanceID), gocheck.IsNil)

	imap.Add(instance2)
	imap.Add(instance3)
	imap.Add(instance4)
	imap.Add(instance5)

	c.Assert(imap.Get(instance1.InstanceID), gocheck.NotNil)
	c.Assert(imap.Get(instance2.InstanceID), gocheck.NotNil)
	c.Assert(imap.Get(instance3.InstanceID), gocheck.NotNil)
	c.Assert(imap.Get(instance4.InstanceID), gocheck.NotNil)
	c.Assert(imap.Get(instance5.InstanceID), gocheck.NotNil)

	c.Assert(imap.Contains(instance1), gocheck.Equals, true)
	c.Assert(imap.Contains(instance2), gocheck.Equals, true)
	c.Assert(imap.Contains(instance3), gocheck.Equals, true)
	c.Assert(imap.Contains(instance4), gocheck.Equals, true)
	c.Assert(imap.Contains(instance5), gocheck.Equals, true)

	c.Assert(imap.ContainsID(instance1.InstanceID), gocheck.Equals, true)
	c.Assert(imap.ContainsID(instance2.InstanceID), gocheck.Equals, true)
	c.Assert(imap.ContainsID(instance3.InstanceID), gocheck.Equals, true)
	c.Assert(imap.ContainsID(instance4.InstanceID), gocheck.Equals, true)
	c.Assert(imap.ContainsID(instance5.InstanceID), gocheck.Equals, true)

	c.Assert(imap.Len(), gocheck.Equals, 5)

	iidSet := NewInstanceIDSet(imap.InstanceIDs())
	c.Assert(iidSet.Contains(instance1.InstanceID), gocheck.Equals, true)
	c.Assert(iidSet.Contains(instance2.InstanceID), gocheck.Equals, true)
	c.Assert(iidSet.Contains(instance3.InstanceID), gocheck.Equals, true)
	c.Assert(iidSet.Contains(instance4.InstanceID), gocheck.Equals, true)
	c.Assert(iidSet.Contains(instance5.InstanceID), gocheck.Equals, true)

	instances := imap.Instances()
	iidSet = NewInstanceIDSet([]InstanceID{})
	for _, inst := range instances {
		iidSet.Add(inst.InstanceID)
	}
	c.Assert(iidSet.Contains(instance1.InstanceID), gocheck.Equals, true)
	c.Assert(iidSet.Contains(instance2.InstanceID), gocheck.Equals, true)
	c.Assert(iidSet.Contains(instance3.InstanceID), gocheck.Equals, true)
	c.Assert(iidSet.Contains(instance4.InstanceID), gocheck.Equals, true)
	c.Assert(iidSet.Contains(instance5.InstanceID), gocheck.Equals, true)
}

func (s *InstanceMapTest) TestGetOrSet(c *gocheck.C) {
	imap := NewInstanceMap()
	instance1 := makeInstance(node.NewNodeId(), []InstanceID{})
	instance2 := makeInstance(node.NewNodeId(), []InstanceID{})
	instance2.InstanceID = instance1.InstanceID

	instance, existed := imap.GetOrSet(instance1, nil)
	c.Assert(existed, gocheck.Equals, false)
	c.Assert(instance, gocheck.Equals, instance1)

	instance, existed = imap.GetOrSet(instance2, nil)
	c.Assert(existed, gocheck.Equals, true)
	c.Assert(instance, gocheck.Equals, instance1)
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
		Command: store.NewInstruction("set", "a", []string{"b", "c"}, time.Now()),
		Dependencies: []InstanceID{NewInstanceID(), NewInstanceID()},
		Status: INSTANCE_ACCEPTED,
		MaxBallot: uint32(500),
		Noop: true,
		commitTimeout: time.Now(),
		executeTimeout: time.Now(),
		DependencyMatch: true,
		ReadOnly: true,
	}
	writer := bufio.NewWriter(buf)
	err = src.Serialize(writer)
	c.Assert(err, gocheck.IsNil)
	err = writer.Flush()
	c.Assert(err, gocheck.IsNil)

	// test num bytes
	c.Check(len(buf.Bytes()), gocheck.Equals, src.NumBytes())

	reader := bufio.NewReader(buf)
	dst := &Instance{}
	err = dst.Deserialize(reader)
	c.Assert(err, gocheck.IsNil)
	c.Assert(dst, gocheck.DeepEquals, src)
}

type InstanceTest struct { }

var _ = gocheck.Suite(&InstanceTest{})

// tests that calling merge attributes fails if the instance
// status id not preaccepted
func (s *InstanceTest) TestMergeAttributesBadStatus(c *gocheck.C) {
	var err error

	instance := makeInstance(node.NewNodeId(), []InstanceID{})

	instance.Status = INSTANCE_PREACCEPTED
	_, err = instance.mergeAttributes([]InstanceID{})
	c.Assert(err, gocheck.IsNil)

	instance.Status = INSTANCE_ACCEPTED
	_, err = instance.mergeAttributes([]InstanceID{})
	c.Assert(err, gocheck.NotNil)

}
