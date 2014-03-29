package consensus

import (
	"time"
)

import (
	"launchpad.net/gocheck"
)

import (
	"store"
)

type DependencyMapTest struct {
	baseManagerTest
}

var _ = gocheck.Suite(&DependencyMapTest{})

func (s *DependencyMapTest) newInstruction(key string) *store.Instruction {
	return store.NewInstruction("SET", key, []string{}, time.Now())
}

// tests that a new dependencies object is created for new root nodes
func (s *DependencyMapTest) TestNewRootDependencyMap(c *gocheck.C) {
	instance := s.manager.makeInstance(s.newInstruction("a"))

	c.Assert(s.manager.depsMngr.deps.deps["a"], gocheck.IsNil)

	deps, err := s.manager.depsMngr.GetAndSetDeps(instance)
	c.Assert(err, gocheck.IsNil)
	c.Assert(deps, gocheck.NotNil)
	c.Assert(deps, gocheck.DeepEquals, []InstanceID{})

	c.Assert(s.manager.depsMngr.deps.deps["a"], gocheck.NotNil)
}

// tests that an existing dependencies object is used for a key if it exists
func (s *DependencyMapTest) TestExistingRootDependencyMap(c *gocheck.C) {
	instance := s.manager.makeInstance(s.newInstruction("a"))

	depsNode := s.manager.depsMngr.deps.get("a")
	lastWrite := NewInstanceID()
	depsNode.lastWrite = lastWrite
	c.Assert(s.manager.depsMngr.deps.deps["a"], gocheck.NotNil)

	deps, err := s.manager.depsMngr.GetAndSetDeps(instance)
	c.Assert(err, gocheck.IsNil)
	c.Assert(deps, gocheck.NotNil)
	c.Assert(deps, gocheck.DeepEquals, []InstanceID{lastWrite})

	c.Assert(s.manager.depsMngr.deps.deps["a"], gocheck.NotNil)
	c.Assert(s.manager.depsMngr.deps.get("a"), gocheck.Equals, depsNode)

}

type DependenciesTest struct {}

var _ = gocheck.Suite(&DependenciesTest{})

// tests that a new dependencies object is created for new leaf nodes
func (s *DependenciesTest) TestNewDependencyMap(c *gocheck.C) {

}

// tests that a new dependencies object is created for new leaf nodes
func (s *DependenciesTest) TestExistingDependencyMap(c *gocheck.C) {

}

// tests the last reads array is updated if the instance is a read
func (s *DependenciesTest) TestLastKeyReadIsUpdated(c *gocheck.C) {

}

// tests the last write is updated if the instance is a write
func (s *DependenciesTest) TestLastKeyWriteIsUpdated(c *gocheck.C) {

}

// tests instances from all child nodes are added to the deps
func (s *DependenciesTest) TestChildDepsAreIncluded(c *gocheck.C) {

}

// tests that child nodes are removed on a write
func (s *DependenciesTest) TestChildrenAreClearedOnWrite(c *gocheck.C) {

}
