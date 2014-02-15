package consensus

import (
	"flag"
	"testing"
	"time"
)

import (
	"launchpad.net/gocheck"
	logging "github.com/op/go-logging"
)

import (
	"store"
	"node"
)

var _test_loglevel = flag.String("test.loglevel", "", "the loglevel to run tests with")

func init() {
	flag.Parse()
}

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {

	// setup test suite logging
	logLevel := logging.CRITICAL
	if *_test_loglevel != "" {
		if level, err := logging.LogLevel(*_test_loglevel); err == nil {
			logLevel = level
		}
	}
	logging.SetLevel(logLevel, "consensus")

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
	c.Assert(s.scope.inProgress.Len(), gocheck.Equals, 4)
	c.Assert(s.scope.committed.Len(), gocheck.Equals, 4)
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
	c.Assert(s.scope.inProgress.Len(), gocheck.Equals, 4)
	c.Assert(s.scope.committed.Len(), gocheck.Equals, 4)
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

//
func (s *ScopeTest) TestGetOrSetNewInstance(c *gocheck.C) {
	instance, existed := s.scope.getOrSetInstance(makeInstance(node.NewNodeId(), []InstanceID{}))
	c.Assert(instance.scope, gocheck.Equals, s.scope)
	c.Assert(existed, gocheck.Equals, false)
}

func (s *ScopeTest) TestGetOrSetExistingInstance(c *gocheck.C) {
	getInst := func(status InstanceStatus) *Instance {
		inst := s.scope.makeInstance(getBasicInstruction())
		inst.Status = status
		return inst
	}
	instance := getInst(INSTANCE_PREACCEPTED)
	s.scope.instances.Add(instance)

	_, existed := s.scope.getOrSetInstance(instance)
	c.Assert(existed, gocheck.Equals, true)
}

// tests that get or set sets new instances to committed if the
// new instance has an status of executed
func (s *ScopeTest) TestGetOrSetResetsToCommitted(c *gocheck.C) {
	getInst := func(status InstanceStatus) *Instance {
		inst := s.scope.makeInstance(getBasicInstruction())
		inst.Status = status
		return inst
	}

	instance, _ := s.scope.getOrSetInstance(getInst(INSTANCE_EXECUTED))
	c.Assert(instance.Status, gocheck.Equals, INSTANCE_COMMITTED)
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
