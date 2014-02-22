package consensus

import (
	"runtime"
	"time"
)


import (
	"launchpad.net/gocheck"
)

type baseIntegrationTest struct {

}

func (t *baseIntegrationTest) SetUpTest(c *gocheck.C) {

}

type PrepareIntegrationTest struct {
	baseReplicaTest
}

var _ = gocheck.Suite(&PrepareIntegrationTest{})

// tests that a prepare phase that receives prepare
// responses with preaccepted instances works as expected
func (s *PrepareIntegrationTest) TestPreparePreAccept(c *gocheck.C) {
	var err error

	// make and accept the instance across the cluster
	instance := s.scope.makeInstance(getBasicInstruction())
	c.Logf("Leader ID: %v", instance.LeaderID)
	c.Logf("num goroutines: %v", runtime.NumGoroutine())
	initialBallot := instance.getBallot()
	shouldAccept, err := s.scope.preAcceptPhase(instance)
	c.Assert(err, gocheck.IsNil)
	c.Assert(shouldAccept, gocheck.Equals, false)
	runtime.Gosched()

	localBallot1 := instance.getBallot()
	c.Assert(localBallot1, gocheck.Equals, initialBallot + 1)
	c.Logf("current ballot: %v", localBallot1)
	c.Logf("num replicas: %v", len(s.replicas))
	c.Logf("num goroutines: %v", runtime.NumGoroutine())

	// check that all the replicas got the message
	for _, replica := range s.replicas {
		scope := replica.manager.getScope(s.scope.name)
		replicaInstance := scope.instances.Get(instance.InstanceID)
		c.Logf("replica %v ballot: %v", replica.id, replicaInstance.getBallot())
		c.Assert(replicaInstance, gocheck.NotNil)
		c.Assert(replicaInstance, gocheck.Not(gocheck.Equals), instance)
		c.Assert(replicaInstance.getBallot(), gocheck.Equals, localBallot1)
		c.Assert(replicaInstance.getStatus(), gocheck.Equals, INSTANCE_PREACCEPTED)
	}

	// set all commit timeouts to now
	for _, n := range s.nodes {
		inst := n.manager.getScope(s.scope.name).getInstance(instance.InstanceID)
		inst.commitTimeout = time.Now()
	}

	successor := s.nodeMap[instance.Successors[0]]
	successorScope := successor.manager.getScope(s.scope.name)

	// run prepare
	c.Logf("\n\nStarting Prepare")
	c.Logf("Successor is: %v", successor.id)
	c.Logf("num goroutines: %v", runtime.NumGoroutine())
	for _, n := range s.nodes {
		scope := n.manager.getScope(s.scope.name)
		replicaInstance := scope.instances.Get(instance.InstanceID)
		c.Logf("replica %v ballot: %v", n.id, replicaInstance.getBallot())
	}
	err = successorScope.preparePhase(instance)
	c.Assert(err, gocheck.IsNil)
}

// tests that a prepare phase that receives prepare
// responses with accepted instances works as expected
func (s *PrepareIntegrationTest) TestPrepareAccept(c *gocheck.C) {
	var err error

	// make and accept the instance across the cluster
	instance := s.scope.makeInstance(getBasicInstruction())
	initialBallot := instance.getBallot()
	err = s.scope.acceptPhase(instance)
	c.Assert(err, gocheck.IsNil)
	runtime.Gosched()

	localBallot1 := instance.getBallot()
	c.Assert(localBallot1, gocheck.Equals, initialBallot + 1)

	// check that all the replicas got the message
	for _, replica := range s.replicas {
		scope := replica.manager.getScope(s.scope.name)
		replicaInstance := scope.instances.Get(instance.InstanceID)
		c.Assert(replicaInstance, gocheck.NotNil)
		c.Assert(replicaInstance.getBallot(), gocheck.Equals, localBallot1)
		c.Assert(replicaInstance.getStatus(), gocheck.Equals, INSTANCE_ACCEPTED)
	}

	// set all commit timeouts to now
	for _, n := range s.nodes {
		inst := n.manager.getScope(s.scope.name).getInstance(instance.InstanceID)
		inst.commitTimeout = time.Now()
	}

	successor := s.nodeMap[instance.Successors[0]]
	successorScope := successor.manager.getScope(s.scope.name)

	// run prepare
	err = successorScope.preparePhase(instance)
	c.Assert(instance.getBallot(), gocheck.Equals, localBallot1 + 1)
	c.Assert(err, gocheck.IsNil)
}

