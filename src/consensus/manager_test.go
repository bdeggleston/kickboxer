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

func TestGetScope(t *testing.T) {

}

func TestCheckScopeEligibility(t *testing.T) {

}

func TestGetScopeNodes(t *testing.T) {

}

func TestGetScopeReplicas(t *testing.T) {

}

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
	instance := s.manager.makeInstance(instructions)
	c.Check(instance.MaxBallot, gocheck.Equals, uint32(0))
	c.Check(instance.LeaderID, gocheck.Equals, s.manager.GetLocalID())

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
	setupDeps(s.manager)
	instructions := getBasicInstruction()
	expected := NewInstanceIDSet([]InstanceID{})
	expected.Add(s.manager.inProgress.InstanceIDs()...)
	expected.Add(s.manager.committed.InstanceIDs()...)

	// sanity checks
	c.Assert(s.manager.inProgress.Len(), gocheck.Equals, 4)
	c.Assert(s.manager.committed.Len(), gocheck.Equals, 4)
	c.Assert(len(s.manager.executed), gocheck.Equals, 4)

	actual := NewInstanceIDSet(s.manager.getInstructionDeps(instructions))

	c.Assert(actual, gocheck.DeepEquals, expected)
}

// tests that manager doesn't try to add executed instances
// if no instances have been executed yet
func (s *ScopeTest) TestGetDepsNoExecutions(c *gocheck.C) {
	setupDeps(s.manager)
	instructions := getBasicInstruction()
	s.manager.executed = []InstanceID{}
	expected := NewInstanceIDSet(s.manager.inProgress.InstanceIDs())
	expected.Add(s.manager.committed.InstanceIDs()...)

	// sanity checks
	c.Assert(s.manager.inProgress.Len(), gocheck.Equals, 4)
	c.Assert(s.manager.committed.Len(), gocheck.Equals, 4)
	c.Assert(len(s.manager.executed), gocheck.Equals, 0)

	actual := NewInstanceIDSet(s.manager.getInstructionDeps(instructions))

	c.Assert(expected.Equal(actual), gocheck.Equals, true)
}

func (s *ScopeTest) TestGetNextSeq(c *gocheck.C) {
	s.manager.maxSeq = 5
	nextSeq := s.manager.getNextSeqUnsafe()

	c.Assert(nextSeq, gocheck.Equals, uint64(6))
}

// tests that the addMissingInstance method works properly
func (s *ScopeTest) TestAddMissingInstance(c *gocheck.C) {
	var err error
	getInst := func(status InstanceStatus) *Instance {
		inst := s.manager.makeInstance(getBasicInstruction())
		inst.Status = status
		return inst
	}

	// preaccepted
	preAccepted := getInst(INSTANCE_PREACCEPTED)
	c.Assert(s.manager.instances.Contains(preAccepted), gocheck.Equals, false)
	err = s.manager.addMissingInstances(preAccepted)
	c.Assert(err, gocheck.IsNil)
	c.Assert(s.manager.instances.Contains(preAccepted), gocheck.Equals, true)
	c.Assert(s.manager.inProgress.Contains(preAccepted), gocheck.Equals, true)
	c.Assert(preAccepted.Status, gocheck.Equals, INSTANCE_PREACCEPTED)

	// accepted
	accepted := getInst(INSTANCE_ACCEPTED)
	c.Assert(s.manager.instances.Contains(accepted), gocheck.Equals, false)
	err = s.manager.addMissingInstances(accepted)
	c.Assert(err, gocheck.IsNil)
	c.Assert(s.manager.instances.Contains(accepted), gocheck.Equals, true)
	c.Assert(s.manager.inProgress.Contains(accepted), gocheck.Equals, true)
	c.Assert(accepted.Status, gocheck.Equals, INSTANCE_ACCEPTED)

	// committed
	committed := getInst(INSTANCE_COMMITTED)
	c.Assert(s.manager.instances.Contains(committed), gocheck.Equals, false)
	err = s.manager.addMissingInstances(committed)
	c.Assert(err, gocheck.IsNil)
	c.Assert(s.manager.instances.Contains(committed), gocheck.Equals, true)
	c.Assert(s.manager.committed.Contains(committed), gocheck.Equals, true)
	c.Assert(committed.Status, gocheck.Equals, INSTANCE_COMMITTED)

	// executed
	executed := getInst(INSTANCE_EXECUTED)
	c.Assert(s.manager.instances.Contains(executed), gocheck.Equals, false)
	err = s.manager.addMissingInstances(executed)
	c.Assert(err, gocheck.IsNil)
	c.Assert(s.manager.instances.Contains(executed), gocheck.Equals, true)
	c.Assert(s.manager.committed.Contains(executed), gocheck.Equals, true)
	c.Assert(len(s.manager.executed), gocheck.Equals, 0)
	c.Assert(executed.Status, gocheck.Equals, INSTANCE_COMMITTED)
}

//
func (s *ScopeTest) TestGetOrSetNewInstance(c *gocheck.C) {
	instance, existed := s.manager.getOrSetInstance(makeInstance(node.NewNodeId(), []InstanceID{}))
	c.Assert(instance.manager, gocheck.Equals, s.manager)
	c.Assert(existed, gocheck.Equals, false)
}

func (s *ScopeTest) TestGetOrSetExistingInstance(c *gocheck.C) {
	getInst := func(status InstanceStatus) *Instance {
		inst := s.manager.makeInstance(getBasicInstruction())
		inst.Status = status
		return inst
	}
	instance := getInst(INSTANCE_PREACCEPTED)
	s.manager.instances.Add(instance)

	_, existed := s.manager.getOrSetInstance(instance)
	c.Assert(existed, gocheck.Equals, true)
}

// tests that get or set sets new instances to committed if the
// new instance has an status of executed
func (s *ScopeTest) TestGetOrSetResetsToCommitted(c *gocheck.C) {
	getInst := func(status InstanceStatus) *Instance {
		inst := s.manager.makeInstance(getBasicInstruction())
		inst.Status = status
		return inst
	}

	instance, _ := s.manager.getOrSetInstance(getInst(INSTANCE_EXECUTED))
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
