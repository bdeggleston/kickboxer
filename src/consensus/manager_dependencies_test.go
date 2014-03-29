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

type baseDependencyTest struct {
	baseManagerTest
}

func (s *baseDependencyTest) newInstruction(key string) *store.Instruction {
	return store.NewInstruction("SET", key, []string{}, time.Now())
}

type DependencyMapTest struct {
	baseDependencyTest
}

var _ = gocheck.Suite(&DependencyMapTest{})

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

type DependenciesTest struct {
	baseDependencyTest
}

var _ = gocheck.Suite(&DependenciesTest{})

// tests that a new dependencies object is created for new leaf nodes
func (s *DependenciesTest) TestNewDependencyMap(c *gocheck.C) {
	instance := s.manager.makeInstance(s.newInstruction("a:b"))
	keys := []string{"a", "b"}
	deps := newDependencies()

	c.Assert(len(deps.subDependencies.deps), gocheck.Equals, 0)
	c.Assert(deps.subDependencies.deps["b"], gocheck.IsNil)

	deps.GetAndSetDeps(keys, instance)

	c.Assert(len(deps.subDependencies.deps), gocheck.Equals, 1)
	c.Assert(deps.subDependencies.deps["b"], gocheck.NotNil)
}

// tests that existing dependencies object is used if it exists
func (s *DependenciesTest) TestExistingDependencyMap(c *gocheck.C) {
	instance := s.manager.makeInstance(s.newInstruction("a:b"))
	keys := []string{"a", "b"}
	deps := newDependencies()


	bdeps := deps.subDependencies.get("b")
	c.Assert(len(deps.subDependencies.deps), gocheck.Equals, 1)
	c.Assert(deps.subDependencies.deps["b"], gocheck.NotNil)

	deps.GetAndSetDeps(keys, instance)

	c.Assert(len(deps.subDependencies.deps), gocheck.Equals, 1)
	c.Assert(deps.subDependencies.deps["b"], gocheck.NotNil)
	c.Assert(deps.subDependencies.get("b"), gocheck.Equals, bdeps)
}

// tests the last reads array is updated if the instance is a read
func (s *DependenciesTest) TestLastKeyReadIsUpdated(c *gocheck.C) {
	instance := s.manager.makeInstance(s.newInstruction("a"))
	instance.ReadOnly = true
	keys := []string{"a"}
	deps := newDependencies()

	c.Assert(deps.lastWrite.IsZero(), gocheck.Equals, true)
	c.Assert(deps.lastReads, gocheck.DeepEquals, []InstanceID{})

	deps.GetAndSetDeps(keys, instance)

	expectedReads := []InstanceID{instance.InstanceID}
	c.Assert(deps.lastWrite.IsZero(), gocheck.Equals, true)
	c.Assert(deps.lastReads, gocheck.DeepEquals, expectedReads)
}

// tests the last write is updated if the instance is a write
func (s *DependenciesTest) TestLastKeyWriteIsUpdated(c *gocheck.C) {
	instance := s.manager.makeInstance(s.newInstruction("a"))
	keys := []string{"a"}
	deps := newDependencies()

	c.Assert(deps.lastWrite.IsZero(), gocheck.Equals, true)
	c.Assert(deps.lastReads, gocheck.DeepEquals, []InstanceID{})

	deps.GetAndSetDeps(keys, instance)

	c.Assert(deps.lastWrite, gocheck.Equals, instance.InstanceID)
	c.Assert(deps.lastReads, gocheck.DeepEquals, []InstanceID{})
}

// tests instances from all child nodes are added to the deps
func (s *DependenciesTest) TestChildDepsAreIncluded(c *gocheck.C) {

}

// tests that child nodes are removed on a write
func (s *DependenciesTest) TestChildrenAreClearedOnWrite(c *gocheck.C) {

}
