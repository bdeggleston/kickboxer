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

func TestGetManager(t *testing.T) {

}

func TestCheckManagerEligibility(t *testing.T) {

}

func TestGetManagerNodes(t *testing.T) {

}

func TestGetManagerReplicas(t *testing.T) {

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

type ManagerTest struct {
	baseReplicaTest
}

var _ = gocheck.Suite(&ManagerTest{})

// test that instances are created properly
func (s *ManagerTest) TestInstanceCreation(c *gocheck.C) {
	instruction := store.NewInstruction("set", "b", []string{}, time.Now())
	instance := s.manager.makeInstance(instruction)
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

// tests that the addMissingInstance method works properly
func (s *ManagerTest) TestAddMissingInstance(c *gocheck.C) {
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
	c.Assert(preAccepted.Status, gocheck.Equals, INSTANCE_PREACCEPTED)

	// accepted
	accepted := getInst(INSTANCE_ACCEPTED)
	c.Assert(s.manager.instances.Contains(accepted), gocheck.Equals, false)
	err = s.manager.addMissingInstances(accepted)
	c.Assert(err, gocheck.IsNil)
	c.Assert(s.manager.instances.Contains(accepted), gocheck.Equals, true)
	c.Assert(accepted.Status, gocheck.Equals, INSTANCE_ACCEPTED)

	// committed
	committed := getInst(INSTANCE_COMMITTED)
	c.Assert(s.manager.instances.Contains(committed), gocheck.Equals, false)
	err = s.manager.addMissingInstances(committed)
	c.Assert(err, gocheck.IsNil)
	c.Assert(s.manager.instances.Contains(committed), gocheck.Equals, true)
	c.Assert(committed.Status, gocheck.Equals, INSTANCE_COMMITTED)

	// executed
	executed := getInst(INSTANCE_EXECUTED)
	c.Assert(s.manager.instances.Contains(executed), gocheck.Equals, false)
	err = s.manager.addMissingInstances(executed)
	c.Assert(err, gocheck.IsNil)
	c.Assert(s.manager.instances.Contains(executed), gocheck.Equals, true)
	c.Assert(len(s.manager.executed), gocheck.Equals, 0)
	c.Assert(executed.Status, gocheck.Equals, INSTANCE_COMMITTED)
}

//
func (s *ManagerTest) TestGetOrSetNewInstance(c *gocheck.C) {
	instance, existed := s.manager.getOrSetInstance(makeInstance(node.NewNodeId(), []InstanceID{}))
	c.Assert(instance.manager, gocheck.Equals, s.manager)
	c.Assert(existed, gocheck.Equals, false)
}

func (s *ManagerTest) TestGetOrSetExistingInstance(c *gocheck.C) {
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

// tests that new instances passed into get or set adds the instance
// to the dependency manager if their status is above preaccepted
func (s *ManagerTest) TestGetOrSetNewInstanceAddsToDepsManager(c *gocheck.C) {
	instance := makeInstance(node.NewNodeId(), []InstanceID{})
	instance.Status = INSTANCE_ACCEPTED
	depsNode := s.manager.depsMngr.deps.get("a")
	c.Assert(depsNode.writes.Contains(instance.InstanceID), gocheck.Equals, false)

	_, existed := s.manager.getOrSetInstance(instance)
	c.Assert(existed, gocheck.Equals, false)

	c.Assert(depsNode.writes.Contains(instance.InstanceID), gocheck.Equals, true)
}

// tests that new instances passed into get or set don't add the instance
// to the dependency manager if the status is preaccepted or below
func (s *ManagerTest) TestGetOrSetPreacceptedNewInstanceSkipsDepsManager(c *gocheck.C) {
	instance := makeInstance(node.NewNodeId(), []InstanceID{})
	instance.Status = INSTANCE_PREACCEPTED
	depsNode := s.manager.depsMngr.deps.get("a")
	c.Assert(depsNode.writes.Contains(instance.InstanceID), gocheck.Equals, false)

	_, existed := s.manager.getOrSetInstance(instance)
	c.Assert(existed, gocheck.Equals, false)

	c.Assert(depsNode.writes.Contains(instance.InstanceID), gocheck.Equals, false)
}

// tests that get or set sets new instances to committed if the
// new instance has an status of executed
func (s *ManagerTest) TestGetOrSetResetsToCommitted(c *gocheck.C) {
	getInst := func(status InstanceStatus) *Instance {
		inst := s.manager.makeInstance(getBasicInstruction())
		inst.Status = status
		return inst
	}

	instance, _ := s.manager.getOrSetInstance(getInst(INSTANCE_EXECUTED))
	c.Assert(instance.Status, gocheck.Equals, INSTANCE_COMMITTED)
}

type ManagerExecuteQueryTest struct {
	baseManagerTest
}

var _ = gocheck.Suite(&ManagerExecuteQueryTest{})

func (s *ManagerExecuteQueryTest) TestPreAcceptSuccess(c *gocheck.C) {

}

func (s *ManagerExecuteQueryTest) TestPreAcceptBallotFailure(c *gocheck.C) {

}

func (s *ManagerExecuteQueryTest) TestAcceptSuccess(c *gocheck.C) {

}

func (s *ManagerExecuteQueryTest) TestAcceptBallotFailure(c *gocheck.C) {

}
