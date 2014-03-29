package consensus

import (
	"launchpad.net/gocheck"
)

type DependencyMapTest struct {}

var _ = gocheck.Suite(&DependencyMapTest{})

// tests that a new dependencies object is created for new root nodes
func (s *DependencyMapTest) TestNewRootDependencyMap(c *gocheck.C) {

}

// tests that an existing dependencies object is used for a key if it exists
func (s *DependencyMapTest) TestExistingRootDependencyMap(c *gocheck.C) {

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
