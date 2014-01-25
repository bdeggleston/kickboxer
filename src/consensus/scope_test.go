package consensus

import (
	"testing"
	"time"
)

import (
	"launchpad.net/gocheck"
)

import (
	"store"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	gocheck.TestingT(t)
}

type ScopeTest struct {
	baseScopeTest
}

var _ = gocheck.Suite(&ScopeTest{})

// test that instances are created properly
func (s *ScopeTest) TestInstanceCreation(c *gocheck.C) {
	instructions := []*store.Instruction{store.NewInstruction("set", "b", []string{}, time.Now())}
	instance := s.scope.makeInstance(instructions)
	c.Check(instance.MaxBallot, gocheck.Equals, uint32(0))
	c.Check(instance.LeaderID, gocheck.Equals, s.scope.GetLocalID())
}

func (s *ScopeTest) TestGetCurrentDeps(c *gocheck.C) {
	setupDeps(s.scope)
	expected := NewInstanceIDSet([]InstanceID{})
	expected.Add(s.scope.inProgress.InstanceIDs()...)
	expected.Add(s.scope.committed.InstanceIDs()...)
	expected.Add(s.scope.executed[len(s.scope.executed)-1])

	// sanity checks
	c.Assert(len(s.scope.inProgress), gocheck.Equals, 4)
	c.Assert(len(s.scope.committed), gocheck.Equals, 4)
	c.Assert(len(s.scope.executed), gocheck.Equals, 4)

	actual := NewInstanceIDSet(s.scope.getCurrentDepsUnsafe())

	c.Assert(expected.Equal(actual), gocheck.Equals, true)
}

// tests that scope doesn't try to add executed instances
// if no instances have been executed yet
func (s *ScopeTest) TestGetDepsNoExecutions(c *gocheck.C) {
	setupDeps(s.scope)
	s.scope.executed = []InstanceID{}
	expected := NewInstanceIDSet(s.scope.inProgress.InstanceIDs())
	expected.Add(s.scope.committed.InstanceIDs()...)

	// sanity checks
	c.Assert(len(s.scope.inProgress), gocheck.Equals, 4)
	c.Assert(len(s.scope.committed), gocheck.Equals, 4)
	c.Assert(len(s.scope.executed), gocheck.Equals, 0)

	actual := NewInstanceIDSet(s.scope.getCurrentDepsUnsafe())

	c.Assert(expected.Equal(actual), gocheck.Equals, true)
}

func (s *ScopeTest) TestGetNextSeq(c *gocheck.C) {
	s.scope.maxSeq = 5
	nextSeq := s.scope.getNextSeqUnsafe()

	c.Assert(nextSeq, gocheck.Equals, uint64(6))
}

// tests that the addMissingInstance method works properly
func (s *ScopeTest) TestAddMissingInstance(c *gocheck.C) {
	// TODO: this
}

type ScopeExecuteQueryTest struct {
	baseScopeTest
}

var _ = gocheck.Suite(&ScopeExecuteQueryTest{})

func (s *ScopeExecuteQueryTest) TestPreAcceptSuccess(c *gocheck.C) {

}

func (s *ScopeExecuteQueryTest) TestPreAcceptBallotFailure(c *gocheck.C) {

}

func (s *ScopeExecuteQueryTest) TestAcceptSuccess(c *gocheck.C) {

}

func (s *ScopeExecuteQueryTest) TestAcceptBallotFailure(c *gocheck.C) {

}
