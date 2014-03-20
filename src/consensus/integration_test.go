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

// waits for all managers to have the given status for the given instance
func (s *baseIntegrationTest) waitForStatus(iid InstanceID, status InstanceStatus) {
	for _, manager := range s.managers {
		if instance := manager.instances.Get(iid); instance == nil {
			runtime.Gosched()
		} else if instance.getStatus() != INSTANCE_PREACCEPTED {
			runtime.Gosched()
		}
	}
}

type PreAcceptIntegrationTest struct {
	baseIntegrationTest
}

var _ = gocheck.Suite(&PreAcceptIntegrationTest{})

func (s *PreAcceptIntegrationTest) TestSuccessCase(c *gocheck.C) {
	// make a pre-existing instance
	knownInstance := s.manager.makeInstance(s.makeInstruction(0))
	for _, manager := range s.managers {
		inst, err := knownInstance.Copy()
		c.Assert(err, gocheck.IsNil)
		err = manager.preAcceptInstance(inst, false)
		c.Assert(err, gocheck.IsNil)
		c.Assert(manager.instances.Get(knownInstance.InstanceID), gocheck.NotNil)
	}

	// run a preaccept phase on a new instance
	newInstance := s.replicaManagers[0].makeInstance(s.makeInstruction(2))
	shouldAccept, err := s.manager.preAcceptPhase(newInstance)
	c.Assert(shouldAccept, gocheck.Equals, false)
	c.Assert(err, gocheck.IsNil)

	s.waitForStatus(newInstance.InstanceID, INSTANCE_PREACCEPTED)

	for _, manager := range s.managers {
		instance := manager.getInstance(newInstance.InstanceID)
		c.Assert(instance, gocheck.NotNil)
		c.Assert(instance.getStatus(), gocheck.Equals, INSTANCE_PREACCEPTED)
	}
	c.Assert(s.manager.instances.Get(newInstance.InstanceID), gocheck.Equals, newInstance)
}

func (s *PreAcceptIntegrationTest) TestMissingInstanceSuccessCase(c *gocheck.C) {
	// make a pre-existing instance
	knownInstance := s.manager.makeInstance(s.makeInstruction(0))
	for _, manager := range s.managers {
		inst, err := knownInstance.Copy()
		c.Assert(err, gocheck.IsNil)
		err = manager.preAcceptInstance(inst, false)
		c.Assert(err, gocheck.IsNil)
		c.Assert(manager.instances.Get(knownInstance.InstanceID), gocheck.NotNil)
	}

	// make an instance known by a subset of replicas
	remoteInstance := s.replicaManagers[0].makeInstance(s.makeInstruction(1))
	quorumSize := (len(s.replicas) / 2) + 1
	c.Assert(quorumSize < len(s.replicas), gocheck.Equals, true)
	for i:=0; i<quorumSize; i++ {
		inst, err := remoteInstance.Copy()
		c.Assert(err, gocheck.IsNil)
		err = s.replicaManagers[i].preAcceptInstance(inst, false)
		c.Assert(err, gocheck.IsNil)
	}

	// run a preaccept phase on a new instance
	newInstance := s.replicaManagers[0].makeInstance(s.makeInstruction(2))
	shouldAccept, err := s.manager.preAcceptPhase(newInstance)
	c.Assert(shouldAccept, gocheck.Equals, true)
	c.Assert(err, gocheck.IsNil)

	// check that the new instance contains the remote instance in it's dependencies
	c.Assert(newInstance.Dependencies, instIdSliceContains, remoteInstance.InstanceID)
	c.Assert(newInstance.Dependencies, instIdSliceContains, knownInstance.InstanceID)

	// check that the remote instance is in the local node
	c.Assert(s.manager.instances.Contains(remoteInstance), gocheck.Equals, true)

	s.waitForStatus(newInstance.InstanceID, INSTANCE_PREACCEPTED)

	for _, manager := range s.managers {
		instance := manager.getInstance(newInstance.InstanceID)
		c.Assert(instance, gocheck.NotNil)
		c.Assert(instance.getStatus(), gocheck.Equals, INSTANCE_PREACCEPTED)
	}
	c.Assert(s.manager.instances.Get(newInstance.InstanceID), gocheck.Equals, newInstance)
}

type AcceptIntegrationTest struct {
	baseIntegrationTest
}

var _ = gocheck.Suite(&AcceptIntegrationTest{})

// test successful accept cycle
func (s *AcceptIntegrationTest) TestAcceptSuccessCase(c *gocheck.C) {
	// make a pre-existing instance
	knownInstance := s.manager.makeInstance(s.makeInstruction(0))
	for _, manager := range s.managers {
		inst, err := knownInstance.Copy()
		c.Assert(err, gocheck.IsNil)
		err = manager.preAcceptInstance(inst, false)
		c.Assert(err, gocheck.IsNil)
		c.Assert(manager.instances.Get(knownInstance.InstanceID), gocheck.NotNil)
	}

	// make an instance known by a subset of replicas
	remoteInstance := s.replicaManagers[0].makeInstance(s.makeInstruction(1))
	quorumSize := (len(s.replicas) / 2) + 1
	c.Assert(quorumSize < len(s.replicas), gocheck.Equals, true)
	for i:=0; i<quorumSize; i++ {
		inst, err := remoteInstance.Copy()
		c.Assert(err, gocheck.IsNil)
		err = s.replicaManagers[i].preAcceptInstance(inst, false)
		c.Assert(err, gocheck.IsNil)
	}

	// run a preaccept phase on a new instance
	newInstance := s.replicaManagers[0].makeInstance(s.makeInstruction(2))
	shouldAccept, err := s.manager.preAcceptPhase(newInstance)
	c.Assert(shouldAccept, gocheck.Equals, true)
	c.Assert(err, gocheck.IsNil)

	s.waitForStatus(newInstance.InstanceID, INSTANCE_PREACCEPTED)

	// check that the new instance contains the remote instance in it's dependencies
	c.Assert(newInstance.Dependencies, instIdSliceContains, remoteInstance.InstanceID)
	c.Assert(newInstance.Dependencies, instIdSliceContains, knownInstance.InstanceID)

	// check that the nodes that preaccepted the remoteInstance have added it to
	// the newInstance deps
	for i, manager := range s.replicaManagers {
		inst := manager.instances.Get(newInstance.InstanceID)
		c.Assert(inst, gocheck.NotNil)
		c.Assert(inst.Dependencies, instIdSliceContains, knownInstance.InstanceID)
		if i < quorumSize {
			c.Assert(inst.Dependencies, instIdSliceContains, remoteInstance.InstanceID)
		} else {
			c.Assert(inst.Dependencies, gocheck.Not(instIdSliceContains), remoteInstance.InstanceID)
		}
	}

	// run an accept phase for the new instance
	err = s.manager.acceptPhase(newInstance)
	c.Assert(err, gocheck.IsNil)

	s.waitForStatus(newInstance.InstanceID, INSTANCE_ACCEPTED)

	// check that all nodes have the remoteInstance in the newInstance deps
	for _, manager := range s.managers {
		inst := manager.instances.Get(newInstance.InstanceID)
		c.Assert(inst, gocheck.NotNil)
		c.Assert(inst.Dependencies, instIdSliceContains, remoteInstance.InstanceID)
	}
	c.Assert(s.manager.instances.Get(newInstance.InstanceID), gocheck.Equals, newInstance)
}

type CommitIntegrationTest struct {
	baseIntegrationTest
}

var _ = gocheck.Suite(&CommitIntegrationTest{})

// test that instances have the right dependency graph when they miss
// an accept message, but receive a commit message with the correct deps
func (s *CommitIntegrationTest) TestSkippedAcceptSuccessCase(c *gocheck.C) {
	// make a pre-existing instance
	knownInstance := s.manager.makeInstance(s.makeInstruction(0))
	for _, manager := range s.managers {
		inst, err := knownInstance.Copy()
		c.Assert(err, gocheck.IsNil)
		err = manager.preAcceptInstance(inst, false)
		c.Assert(err, gocheck.IsNil)
		c.Assert(manager.instances.Get(knownInstance.InstanceID), gocheck.NotNil)
	}

	// make an instance known by a subset of replicas
	remoteInstance := s.replicaManagers[0].makeInstance(s.makeInstruction(1))
	quorumSize := (len(s.replicas) / 2) + 1
	c.Assert(quorumSize < len(s.replicas), gocheck.Equals, true)
	for i:=0; i<quorumSize; i++ {
		inst, err := remoteInstance.Copy()
		c.Assert(err, gocheck.IsNil)
		err = s.replicaManagers[i].preAcceptInstance(inst, false)
		c.Assert(err, gocheck.IsNil)
	}

	// run a preaccept phase on a new instance
	newInstance := s.replicaManagers[0].makeInstance(s.makeInstruction(2))
	shouldAccept, err := s.manager.preAcceptPhase(newInstance)
	c.Assert(shouldAccept, gocheck.Equals, true)
	c.Assert(err, gocheck.IsNil)

	// check that the new instance contains the remote instance in it's dependencies
	c.Assert(newInstance.Dependencies, instIdSliceContains, remoteInstance.InstanceID)
	c.Assert(newInstance.Dependencies, instIdSliceContains, knownInstance.InstanceID)

	// check that the nodes that preaccepted the remoteInstance have added it to
	// the newInstance deps
	for i, manager := range s.replicaManagers {
		inst := manager.instances.Get(newInstance.InstanceID)
		c.Assert(inst, gocheck.NotNil)
		c.Assert(inst.Dependencies, instIdSliceContains, knownInstance.InstanceID)
		if i < quorumSize {
			c.Assert(inst.Dependencies, instIdSliceContains, remoteInstance.InstanceID)
		} else {
			c.Assert(inst.Dependencies, gocheck.Not(instIdSliceContains), remoteInstance.InstanceID)
		}
	}

	// run a commit phase for the new instance
	err = s.manager.commitPhase(newInstance)
	c.Assert(err, gocheck.IsNil)

	s.waitForStatus(newInstance.InstanceID, INSTANCE_COMMITTED)

	// check that all nodes have the remoteInstance in the newInstance deps
	for _, manager := range s.managers {
		inst := manager.instances.Get(newInstance.InstanceID)
		c.Assert(inst, gocheck.NotNil)
		c.Assert(inst.Dependencies, instIdSliceContains, remoteInstance.InstanceID)
	}
	c.Assert(s.manager.instances.Get(newInstance.InstanceID), gocheck.Equals, newInstance)
}


type PrepareIntegrationTest struct {
	baseIntegrationTest
}

var _ = gocheck.Suite(&PrepareIntegrationTest{})

func (s *PrepareIntegrationTest) waitOnStatus(iid InstanceID, status InstanceStatus) {
	for i:=0;i<20;i++ {
		stop := false
		for _, n := range s.nodes {
			manager := n.manager
			if manager.instances.Get(iid).getStatus() == status {
				stop = true
				break
			} else {
				runtime.Gosched()
			}
		}
		if stop {
			break
		}
	}
}

// tests that a prepare phase that receives prepare
// responses with preaccepted instances works as expected
func (s *PrepareIntegrationTest) TestPreparePreAccept(c *gocheck.C) {
	// make and accept the instance across the cluster
	instructions := s.makeInstruction(0)
	instance := s.manager.makeInstance(instructions)
	c.Logf("Leader ID: %v", instance.LeaderID)
	initialBallot := instance.getBallot()
	shouldAccept, err := s.manager.preAcceptPhase(instance)
	c.Assert(err, gocheck.IsNil)
	c.Assert(shouldAccept, gocheck.Equals, false)
	runtime.Gosched()

	localBallot1 := instance.getBallot()
	c.Assert(localBallot1, gocheck.Equals, initialBallot + 1)
	c.Logf("current ballot: %v", localBallot1)
	c.Logf("num replicas: %v", len(s.replicas))

	// check that all the replicas got the message
	for _, replica := range s.replicas {
		replicaInstance := replica.manager.instances.Get(instance.InstanceID)
		c.Logf("replica %v ballot: %v", replica.id, replicaInstance.getBallot())
		c.Assert(replicaInstance, gocheck.NotNil)
		c.Assert(replicaInstance, gocheck.Not(gocheck.Equals), instance)
		c.Assert(replicaInstance.getBallot(), gocheck.Equals, localBallot1)
		c.Assert(replicaInstance.getStatus(), gocheck.Equals, INSTANCE_PREACCEPTED)
	}

	// set all commit timeouts to now
	for _, n := range s.nodes {
		inst := n.manager.getInstance(instance.InstanceID)
		inst.commitTimeout = time.Now()
	}

	successor := s.nodeMap[instance.Successors[0]]

	// run prepare
	c.Logf("\n\nStarting Prepare")
	c.Logf("Successor is: %v", successor.id)
	for _, n := range s.nodes {
		replicaInstance := s.manager.instances.Get(instance.InstanceID)
		c.Logf("replica %v ballot: %v", n.id, replicaInstance.getBallot())
	}
	err = s.manager.preparePhase(instance)
	s.waitForStatus(instance.InstanceID, INSTANCE_COMMITTED)
	c.Assert(err, gocheck.IsNil)
	// ballot +3: prepare, preaccept, and commit messages
	c.Assert(instance.getBallot(), gocheck.Equals, localBallot1 + 3)
	c.Assert(instance.getStatus(), gocheck.Equals, INSTANCE_COMMITTED)
	c.Assert(s.manager.instances.Get(instance.InstanceID), gocheck.Equals, instance)
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
	instance := s.manager.makeInstance(instructions)
	initialBallot := instance.getBallot()
	err = s.manager.acceptPhase(instance)
	c.Assert(err, gocheck.IsNil)
	runtime.Gosched()

	localBallot1 := instance.getBallot()
	c.Assert(localBallot1, gocheck.Equals, initialBallot + 1)
	c.Logf("current ballot: %v", localBallot1)
	c.Logf("num replicas: %v", len(s.replicas))

	// check that all the replicas got the message
	for _, replica := range s.replicas {
		replicaInstance := s.manager.instances.Get(instance.InstanceID)
		c.Logf("replica %v ballot: %v", replica.id, replicaInstance.getBallot())
		c.Assert(replicaInstance, gocheck.NotNil)
		c.Assert(replicaInstance.getBallot(), gocheck.Equals, localBallot1)
		c.Assert(replicaInstance.getStatus(), gocheck.Equals, INSTANCE_ACCEPTED)
	}

	// set all commit timeouts to now
	for _, n := range s.nodes {
		inst := n.manager.getInstance(instance.InstanceID)
		inst.commitTimeout = time.Now()
	}

	successor := s.nodeMap[instance.Successors[0]]

	// run prepare
	c.Logf("\n\nStarting Prepare")
	c.Logf("Successor is: %v", successor.id)
	for _, n := range s.nodes {
		replicaInstance := s.manager.instances.Get(instance.InstanceID)
		c.Logf("replica %v ballot: %v", n.id, replicaInstance.getBallot())
	}
	err = s.manager.preparePhase(instance)
	s.waitForStatus(instance.InstanceID, INSTANCE_COMMITTED)
	c.Assert(err, gocheck.IsNil)
	// ballot +3: prepare, acceptm and commit messages
	c.Assert(instance.getBallot(), gocheck.Equals, localBallot1 + 3)
	c.Assert(instance.getStatus(), gocheck.Equals, INSTANCE_COMMITTED)
	c.Assert(s.manager.instances.Get(instance.InstanceID), gocheck.Equals, instance)
}

