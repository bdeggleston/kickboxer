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
	"node"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	gocheck.TestingT(t)
}

type ScopeTest struct {
	baseReplicaTest
}

var _ = gocheck.Suite(&ScopeTest{})

// test that instances are created properly
func (s *ScopeTest) TestInstanceCreation(c *gocheck.C) {
	instructions := []*store.Instruction{store.NewInstruction("set", "b", []string{}, time.Now())}
	instance := s.scope.makeInstance(instructions)
	c.Check(instance.MaxBallot, gocheck.Equals, uint32(0))
	c.Check(instance.LeaderID, gocheck.Equals, s.scope.GetLocalID())

	// check successors
	c.Assert(len(instance.Successors), gocheck.Equals, s.numNodes - 1)
	expected := make(map[node.NodeId]bool)
	for _, n := range s.nodes {
		if n.GetId() != s.manager.GetLocalID() {
			expected[n.GetId()] = true
		}
	}
	actual := make(map[node.NodeId]bool)
	for _, nid := range instance.Successors {
		actual[nid] = true
	}
	c.Check(actual, gocheck.DeepEquals, expected)
}

func (s *ScopeTest) TestGetCurrentDeps(c *gocheck.C) {
	setupDeps(s.scope)
	expected := NewInstanceIDSet([]InstanceID{})
	expected.Add(s.scope.inProgress.InstanceIDs()...)
	expected.Add(s.scope.committed.InstanceIDs()...)
	expected.Add(s.scope.executed...)

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
	var err error
	getInst := func(status InstanceStatus) *Instance {
		inst := s.scope.makeInstance(getBasicInstruction())
		inst.Status = status
		return inst
	}

	// preaccepted
	preAccepted := getInst(INSTANCE_PREACCEPTED)
	c.Assert(s.scope.instances.Contains(preAccepted), gocheck.Equals, false)
	err = s.scope.addMissingInstances(preAccepted)
	c.Assert(err, gocheck.IsNil)
	c.Assert(s.scope.instances.Contains(preAccepted), gocheck.Equals, true)
	c.Assert(s.scope.inProgress.Contains(preAccepted), gocheck.Equals, true)
	c.Assert(preAccepted.Status, gocheck.Equals, INSTANCE_PREACCEPTED)

	// accepted
	accepted := getInst(INSTANCE_ACCEPTED)
	c.Assert(s.scope.instances.Contains(accepted), gocheck.Equals, false)
	err = s.scope.addMissingInstances(accepted)
	c.Assert(err, gocheck.IsNil)
	c.Assert(s.scope.instances.Contains(accepted), gocheck.Equals, true)
	c.Assert(s.scope.inProgress.Contains(accepted), gocheck.Equals, true)
	c.Assert(accepted.Status, gocheck.Equals, INSTANCE_ACCEPTED)

	// committed
	committed := getInst(INSTANCE_COMMITTED)
	c.Assert(s.scope.instances.Contains(committed), gocheck.Equals, false)
	err = s.scope.addMissingInstances(committed)
	c.Assert(err, gocheck.IsNil)
	c.Assert(s.scope.instances.Contains(committed), gocheck.Equals, true)
	c.Assert(s.scope.committed.Contains(committed), gocheck.Equals, true)
	c.Assert(committed.Status, gocheck.Equals, INSTANCE_COMMITTED)

	// executed
	executed := getInst(INSTANCE_EXECUTED)
	c.Assert(s.scope.instances.Contains(executed), gocheck.Equals, false)
	err = s.scope.addMissingInstances(executed)
	c.Assert(err, gocheck.IsNil)
	c.Assert(s.scope.instances.Contains(executed), gocheck.Equals, true)
	c.Assert(s.scope.committed.Contains(executed), gocheck.Equals, true)
	c.Assert(len(s.scope.executed), gocheck.Equals, 0)
	c.Assert(executed.Status, gocheck.Equals, INSTANCE_COMMITTED)
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
