package consensus

import (
	"fmt"
	"runtime"
	"time"
)

import (
	"launchpad.net/gocheck"
)

import (
	"store"
)

type baseIntegrationTest struct {
	baseReplicaTest
}

func (s *baseIntegrationTest) SetUpTest(c *gocheck.C) {
	s.baseReplicaTest.SetUpTest(c)
}

func (s *baseIntegrationTest) makeInstruction(val int) []*store.Instruction {
	return []*store.Instruction{
		store.NewInstruction("set", "a", []string{fmt.Sprint(val)}, time.Now()),
	}
}

type PreAcceptIntegrationTest struct {
	baseIntegrationTest
}

var _ = gocheck.Suite(&PreAcceptIntegrationTest{})

func (s *PreAcceptIntegrationTest) TestSuccessCase(c *gocheck.C) {
	// make a pre-existing instance
	knownInstance := s.scope.makeInstance(s.makeInstruction(0))
	for _, scope := range s.scopes {
		inst, err := knownInstance.Copy()
		c.Assert(err, gocheck.IsNil)
		err = scope.preAcceptInstance(inst, false)
		c.Assert(err, gocheck.IsNil)
		c.Assert(scope.instances.Get(knownInstance.InstanceID), gocheck.NotNil)
	}

	// run a preaccept phase on a new instance
	newInstance := s.replicaScopes[0].makeInstance(s.makeInstruction(2))
	shouldAccept, err := s.scope.preAcceptPhase(newInstance)
	c.Assert(shouldAccept, gocheck.Equals, false)
	c.Assert(err, gocheck.IsNil)

	for _, scope := range s.scopes {
		instance := scope.getInstance(newInstance.InstanceID)
		c.Assert(instance, gocheck.NotNil)
		c.Assert(instance.getStatus(), gocheck.Equals, INSTANCE_PREACCEPTED)
	}
}

func (s *PreAcceptIntegrationTest) TestMissingInstanceSuccessCase(c *gocheck.C) {
	// make a pre-existing instance
	knownInstance := s.scope.makeInstance(s.makeInstruction(0))
	for _, scope := range s.scopes {
		inst, err := knownInstance.Copy()
		c.Assert(err, gocheck.IsNil)
		err = scope.preAcceptInstance(inst, false)
		c.Assert(err, gocheck.IsNil)
		c.Assert(scope.instances.Get(knownInstance.InstanceID), gocheck.NotNil)
	}

	// make an instance known by a subset of replicas
	remoteInstance := s.replicaScopes[0].makeInstance(s.makeInstruction(1))
	quorumSize := (len(s.replicas) / 2) + 1
	c.Assert(quorumSize < len(s.replicas), gocheck.Equals, true)
	for i:=0; i<quorumSize; i++ {
		inst, err := remoteInstance.Copy()
		c.Assert(err, gocheck.IsNil)
		err = s.replicaScopes[i].preAcceptInstance(inst, false)
		c.Assert(err, gocheck.IsNil)
	}

	// run a preaccept phase on a new instance
	newInstance := s.replicaScopes[0].makeInstance(s.makeInstruction(2))
	shouldAccept, err := s.scope.preAcceptPhase(newInstance)
	c.Assert(shouldAccept, gocheck.Equals, true)
	c.Assert(err, gocheck.IsNil)

	// check that the new instance contains the remote instance in it's dependencies
	c.Assert(newInstance.Dependencies, instIdSliceContains, remoteInstance.InstanceID)
	c.Assert(newInstance.Dependencies, instIdSliceContains, knownInstance.InstanceID)

	// check that the remote instance is in the local node
	c.Assert(s.scope.instances.Contains(remoteInstance), gocheck.Equals, true)

	for _, scope := range s.scopes {
		instance := scope.getInstance(newInstance.InstanceID)
		c.Assert(instance, gocheck.NotNil)
		c.Assert(instance.getStatus(), gocheck.Equals, INSTANCE_PREACCEPTED)
	}
}

type AcceptIntegrationTest struct {
	baseIntegrationTest
}

var _ = gocheck.Suite(&AcceptIntegrationTest{})

// test successful accept cycle
func (s *AcceptIntegrationTest) TestAcceptSuccessCase(c *gocheck.C) {
	// make a pre-existing instance
	knownInstance := s.scope.makeInstance(s.makeInstruction(0))
	for _, scope := range s.scopes {
		inst, err := knownInstance.Copy()
		c.Assert(err, gocheck.IsNil)
		err = scope.preAcceptInstance(inst, false)
		c.Assert(err, gocheck.IsNil)
		c.Assert(scope.instances.Get(knownInstance.InstanceID), gocheck.NotNil)
	}

	// make an instance known by a subset of replicas
	remoteInstance := s.replicaScopes[0].makeInstance(s.makeInstruction(1))
	quorumSize := (len(s.replicas) / 2) + 1
	c.Assert(quorumSize < len(s.replicas), gocheck.Equals, true)
	for i:=0; i<quorumSize; i++ {
		inst, err := remoteInstance.Copy()
		c.Assert(err, gocheck.IsNil)
		err = s.replicaScopes[i].preAcceptInstance(inst, false)
		c.Assert(err, gocheck.IsNil)
	}

	// run a preaccept phase on a new instance
	newInstance := s.replicaScopes[0].makeInstance(s.makeInstruction(2))
	shouldAccept, err := s.scope.preAcceptPhase(newInstance)
	c.Assert(shouldAccept, gocheck.Equals, true)
	c.Assert(err, gocheck.IsNil)

	// check that the new instance contains the remote instance in it's dependencies
	c.Assert(newInstance.Dependencies, instIdSliceContains, remoteInstance.InstanceID)
	c.Assert(newInstance.Dependencies, instIdSliceContains, knownInstance.InstanceID)

	// check that the nodes that preaccepted the remoteInstance have added it to
	// the newInstance deps
	for i, scope := range s.replicaScopes {
		inst := scope.instances.Get(newInstance.InstanceID)
		c.Assert(inst, gocheck.NotNil)
		c.Assert(inst.Dependencies, instIdSliceContains, knownInstance.InstanceID)
		if i < quorumSize {
			c.Assert(inst.Dependencies, instIdSliceContains, remoteInstance.InstanceID)
		} else {
			c.Assert(inst.Dependencies, gocheck.Not(instIdSliceContains), remoteInstance.InstanceID)
		}
	}

	// run an accept phase for the new instance
	err = s.scope.acceptPhase(newInstance)
	c.Assert(err, gocheck.IsNil)

	// check that all nodes have the remoteInstance in the newInstance deps
	for _, scope := range s.scopes {
		inst := scope.instances.Get(newInstance.InstanceID)
		c.Assert(inst, gocheck.NotNil)
		c.Assert(inst.Dependencies, instIdSliceContains, remoteInstance.InstanceID)
	}

}

type CommitIntegrationTest struct {
	baseIntegrationTest
}

var _ = gocheck.Suite(&CommitIntegrationTest{})

// test that instances have the right dependency graph when they miss
// an accept message, but receive a commit message with the correct deps
func (s *CommitIntegrationTest) TestSkippedAcceptSuccessCase(c *gocheck.C) {
	// make a pre-existing instance
	knownInstance := s.scope.makeInstance(s.makeInstruction(0))
	for _, scope := range s.scopes {
		inst, err := knownInstance.Copy()
		c.Assert(err, gocheck.IsNil)
		err = scope.preAcceptInstance(inst, false)
		c.Assert(err, gocheck.IsNil)
		c.Assert(scope.instances.Get(knownInstance.InstanceID), gocheck.NotNil)
	}

	// make an instance known by a subset of replicas
	remoteInstance := s.replicaScopes[0].makeInstance(s.makeInstruction(1))
	quorumSize := (len(s.replicas) / 2) + 1
	c.Assert(quorumSize < len(s.replicas), gocheck.Equals, true)
	for i:=0; i<quorumSize; i++ {
		inst, err := remoteInstance.Copy()
		c.Assert(err, gocheck.IsNil)
		err = s.replicaScopes[i].preAcceptInstance(inst, false)
		c.Assert(err, gocheck.IsNil)
	}

	// run a preaccept phase on a new instance
	newInstance := s.replicaScopes[0].makeInstance(s.makeInstruction(2))
	shouldAccept, err := s.scope.preAcceptPhase(newInstance)
	c.Assert(shouldAccept, gocheck.Equals, true)
	c.Assert(err, gocheck.IsNil)

	// check that the new instance contains the remote instance in it's dependencies
	c.Assert(newInstance.Dependencies, instIdSliceContains, remoteInstance.InstanceID)
	c.Assert(newInstance.Dependencies, instIdSliceContains, knownInstance.InstanceID)

	// check that the nodes that preaccepted the remoteInstance have added it to
	// the newInstance deps
	for i, scope := range s.replicaScopes {
		inst := scope.instances.Get(newInstance.InstanceID)
		c.Assert(inst, gocheck.NotNil)
		c.Assert(inst.Dependencies, instIdSliceContains, knownInstance.InstanceID)
		if i < quorumSize {
			c.Assert(inst.Dependencies, instIdSliceContains, remoteInstance.InstanceID)
		} else {
			c.Assert(inst.Dependencies, gocheck.Not(instIdSliceContains), remoteInstance.InstanceID)
		}
	}

	// run a commit phase for the new instance
	err = s.scope.commitPhase(newInstance)
	c.Assert(err, gocheck.IsNil)
	runtime.Gosched()

	// check that all nodes have the remoteInstance in the newInstance deps
	for _, scope := range s.scopes {
		inst := scope.instances.Get(newInstance.InstanceID)
		c.Assert(inst, gocheck.NotNil)
		c.Assert(inst.Dependencies, instIdSliceContains, remoteInstance.InstanceID)
	}

}


type PrepareIntegrationTest struct {
	baseIntegrationTest
}

var _ = gocheck.Suite(&PrepareIntegrationTest{})

// tests that a prepare phase that receives prepare
// responses with preaccepted instances works as expected
func (s *PrepareIntegrationTest) TestPreparePreAccept(c *gocheck.C) {
	// make and accept the instance across the cluster
	instructions := s.makeInstruction(0)
	instance := s.scope.makeInstance(instructions)
	c.Logf("Leader ID: %v", instance.LeaderID)
	initialBallot := instance.getBallot()
	shouldAccept, err := s.scope.preAcceptPhase(instance)
	c.Assert(err, gocheck.IsNil)
	c.Assert(shouldAccept, gocheck.Equals, false)
	runtime.Gosched()

	localBallot1 := instance.getBallot()
	c.Assert(localBallot1, gocheck.Equals, initialBallot + 1)
	c.Logf("current ballot: %v", localBallot1)
	c.Logf("num replicas: %v", len(s.replicas))

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
	for _, n := range s.nodes {
		scope := n.manager.getScope(s.scope.name)
		replicaInstance := scope.instances.Get(instance.InstanceID)
		c.Logf("replica %v ballot: %v", n.id, replicaInstance.getBallot())
	}
	err = successorScope.preparePhase(instance)
	c.Assert(err, gocheck.IsNil)
	c.Assert(instance.getBallot(), gocheck.Equals, localBallot1 + 2)
	c.Assert(s.scope.instances.Get(instance.InstanceID), gocheck.Equals, instance)
}

// tests the behavior of a prepare phase with a rejected prepare request
func (s *PrepareIntegrationTest) TestPrepareBallotFailure(c *gocheck.C) {

}

// tests that a prepare phase that receives prepare
// responses with accepted instances works as expected
func (s *PrepareIntegrationTest) TestPrepareAccept(c *gocheck.C) {
	var err error

	// make and accept the instance across the cluster
	instructions := []*store.Instruction{store.NewInstruction("set", "a", []string{fmt.Sprint(0)}, time.Now())}
	instance := s.scope.makeInstance(instructions)
	initialBallot := instance.getBallot()
	err = s.scope.acceptPhase(instance)
	c.Assert(err, gocheck.IsNil)
	runtime.Gosched()

	localBallot1 := instance.getBallot()
	c.Assert(localBallot1, gocheck.Equals, initialBallot + 1)
	c.Logf("current ballot: %v", localBallot1)
	c.Logf("num replicas: %v", len(s.replicas))

	// check that all the replicas got the message
	for _, replica := range s.replicas {
		scope := replica.manager.getScope(s.scope.name)
		replicaInstance := scope.instances.Get(instance.InstanceID)
		c.Logf("replica %v ballot: %v", replica.id, replicaInstance.getBallot())
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
	c.Logf("\n\nStarting Prepare")
	c.Logf("Successor is: %v", successor.id)
	for _, n := range s.nodes {
		scope := n.manager.getScope(s.scope.name)
		replicaInstance := scope.instances.Get(instance.InstanceID)
		c.Logf("replica %v ballot: %v", n.id, replicaInstance.getBallot())
	}
	err = successorScope.preparePhase(instance)
	c.Assert(err, gocheck.IsNil)
	c.Assert(instance.getBallot(), gocheck.Equals, localBallot1 + 2)
	c.Assert(s.scope.instances.Get(instance.InstanceID), gocheck.Equals, instance)
}

