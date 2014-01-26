package consensus

import (
	"launchpad.net/gocheck"
)

type InstanceSetTest struct { }

var _ = gocheck.Suite(&InstanceSetTest{})

func (s *InstanceSetTest) TestSetEqual(c *gocheck.C) {
	ids := []InstanceID{NewInstanceID(), NewInstanceID()}
	set0 := NewInstanceIDSet(ids)
	set1 := NewInstanceIDSet(ids)
	c.Assert(set0.Equal(set1), gocheck.Equals, true)
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

