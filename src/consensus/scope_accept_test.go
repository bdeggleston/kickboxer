package consensus

import (
	"fmt"
	"time"
)

import (
	"launchpad.net/gocheck"
)

import (
	"message"
	"node"
)

type AcceptInstanceTest struct {
	baseScopeTest
}

var _ = gocheck.Suite(&AcceptInstanceTest{})

// tests that an instance is marked as accepted,
// added to the inProgress set, has it's seq & deps
// updated and persisted if it's only preaccepted
func (s *AcceptInstanceTest) TestSuccessCase(c *gocheck.C) {
	replicaInstance := makeInstance(node.NewNodeId(), makeDependencies(4))
	s.scope.maxSeq = 3
	replicaInstance.Sequence = s.scope.maxSeq

	s.scope.instances.Add(replicaInstance)
	s.scope.inProgress.Add(replicaInstance)
	s.scope.maxSeq = replicaInstance.Sequence

	// sanity checks
	c.Assert(4, gocheck.Equals, len(replicaInstance.Dependencies))
	c.Assert(uint64(3), gocheck.Equals, replicaInstance.Sequence)
	c.Assert(uint64(3), gocheck.Equals, s.scope.maxSeq)

	leaderInstance := copyInstance(replicaInstance)
	leaderInstance.Sequence++
	leaderInstance.Dependencies = append(leaderInstance.Dependencies, NewInstanceID())

	err := s.scope.acceptInstance(leaderInstance)
	c.Assert(err, gocheck.IsNil)

	c.Check(INSTANCE_ACCEPTED, gocheck.Equals, replicaInstance.Status)
	c.Check(INSTANCE_ACCEPTED, gocheck.Equals, leaderInstance.Status)
	c.Check(5, gocheck.Equals, len(replicaInstance.Dependencies))
	c.Check(uint64(4), gocheck.Equals, replicaInstance.Sequence)
	c.Check(uint64(4), gocheck.Equals, s.scope.maxSeq)
}

// tests that an instance is marked as accepted,
// added to the instances and inProgress set, and
// persisted if the instance hasn't been seen before
func (s *AcceptInstanceTest) TestNewInstanceSuccess(c *gocheck.C) {
	s.scope.maxSeq = 3

	leaderInstance := makeInstance(node.NewNodeId(), makeDependencies(4))
	leaderInstance.Sequence = s.scope.maxSeq + 2

	// sanity checks
	c.Assert(s.scope.instances.Contains(leaderInstance), gocheck.Equals, false)
	c.Assert(s.scope.inProgress.Contains(leaderInstance), gocheck.Equals, false)
	c.Assert(s.scope.committed.Contains(leaderInstance), gocheck.Equals, false)

	err := s.scope.acceptInstance(leaderInstance)
	c.Assert(err, gocheck.IsNil)

	c.Check(s.scope.instances.Contains(leaderInstance), gocheck.Equals, true)
	c.Check(s.scope.inProgress.Contains(leaderInstance), gocheck.Equals, true)
	c.Check(s.scope.committed.Contains(leaderInstance), gocheck.Equals, false)

	replicaInstance := s.scope.instances[leaderInstance.InstanceID]
	c.Check(replicaInstance.Status, gocheck.Equals, INSTANCE_ACCEPTED)
	c.Check(leaderInstance.Status, gocheck.Equals, INSTANCE_ACCEPTED)
	c.Check(len(replicaInstance.Dependencies), gocheck.Equals, 4)
	c.Check(replicaInstance.Sequence, gocheck.Equals, uint64(5))
	c.Check(s.scope.maxSeq, gocheck.Equals, uint64(5))
}

// tests that an instance is not marked as accepted,
// or added to the inProgress set if it already has
// a higher status
func (s *AcceptInstanceTest) TestHigherStatusFailure(c *gocheck.C) {
	replicaInstance := makeInstance(node.NewNodeId(), makeDependencies(4))
	s.scope.maxSeq = 3
	replicaInstance.Sequence = s.scope.maxSeq
	replicaInstance.Status = INSTANCE_COMMITTED

	s.scope.instances.Add(replicaInstance)
	s.scope.committed.Add(replicaInstance)

	leaderInstance := copyInstance(replicaInstance)
	leaderInstance.Status = INSTANCE_ACCEPTED

	// sanity checks
	c.Assert(s.scope.committed.Contains(leaderInstance), gocheck.Equals, true)
	c.Assert(s.scope.inProgress.Contains(leaderInstance), gocheck.Equals, false)

	err := s.scope.acceptInstance(leaderInstance)
	c.Assert(err, gocheck.FitsTypeOf, InvalidStatusUpdateError{})

	// check set memberships haven't changed
	c.Check(s.scope.inProgress.Contains(leaderInstance), gocheck.Equals, false)
	c.Check(s.scope.committed.Contains(leaderInstance), gocheck.Equals, true)
	c.Check(replicaInstance.Status, gocheck.Equals, INSTANCE_COMMITTED)
}

type AcceptLeaderTest struct {
	baseReplicaTest
	instance *Instance
	oldAcceptTimeout uint64
}

var _ = gocheck.Suite(&AcceptLeaderTest{})

func (s *AcceptLeaderTest) SetUpSuite(c *gocheck.C) {
	s.baseReplicaTest.SetUpSuite(c)
	s.oldAcceptTimeout = ACCEPT_TIMEOUT
	ACCEPT_TIMEOUT = 50
}

func (s *AcceptLeaderTest) TearDownSuite(c *gocheck.C) {
	ACCEPT_TIMEOUT = s.oldAcceptTimeout
}

func (s *AcceptLeaderTest) SetUpTest(c *gocheck.C) {
	s.baseReplicaTest.SetUpTest(c)
	s.instance = s.scope.makeInstance(getBasicInstruction())
	var err error

	err = s.scope.preAcceptInstance(s.instance)
	c.Assert(err, gocheck.IsNil)
	err = s.scope.acceptInstance(s.instance)
	c.Assert(err, gocheck.IsNil)
}

// tests all replicas returning results
func (s *AcceptLeaderTest) TestSendAcceptSuccess(c *gocheck.C) {
	// all replicas agree
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		return &AcceptResponse{
			Accepted:         true,
			MaxBallot:        s.instance.MaxBallot,
		}, nil
	}

	for _, replica := range s.replicas {
		replica.messageHandler = responseFunc
	}

	err := s.scope.sendAccept(s.instance, transformMockNodeArray(s.replicas))
	c.Assert(err, gocheck.IsNil)

	// test that the nodes received the correct message
	for _, replica := range s.replicas {
		c.Assert(len(replica.sentMessages), gocheck.Equals, 1)
		msg := replica.sentMessages[0]
		c.Check(msg, gocheck.FitsTypeOf, &AcceptRequest{})
	}
}

// tests proper error is returned if
// less than a quorum respond
func (s *AcceptLeaderTest) TestQuorumFailure(c *gocheck.C) {
	// all replicas agree
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		return &AcceptResponse{
			Accepted:         true,
			MaxBallot:        s.instance.MaxBallot,
		}, nil
	}
	hangResponse := func(n *mockNode, m message.Message) (message.Message, error) {
		time.Sleep(1 * time.Second)
		return nil, fmt.Errorf("nope")
	}

	for i, replica := range s.replicas {
		if i == 0 {
			replica.messageHandler = responseFunc
		} else {
			replica.messageHandler = hangResponse
		}
	}

	err := s.scope.sendAccept(s.instance, transformMockNodeArray(s.replicas))
	c.Assert(err, gocheck.NotNil)
	c.Check(err, gocheck.FitsTypeOf, TimeoutError{})
}

func (s *AcceptLeaderTest) TestSendAcceptBallotFailure(c *gocheck.C) {
	// TODO: figure out what to do in this situation
	// the only way this would happen if is the command
	// was taken over by another replica, in which case,
	// should we just wait for the other leader to
	// execute it?
	c.Skip("figure out the expected behavior")
}

/** replica **/

type AcceptReplicaTest struct {
	baseScopeTest
	instance *Instance
}

var _ = gocheck.Suite(&AcceptReplicaTest{})

func (s *AcceptReplicaTest) SetUpTest(c *gocheck.C) {
	s.baseScopeTest.SetUpTest(c)
	s.instance = s.scope.makeInstance(getBasicInstruction())
}

// test that instances are marked as accepted when
// an accept request is received, and there are no
// problems with the request
func (s *AcceptReplicaTest) TestHandleSuccessCase(c *gocheck.C) {
	var err error

	err = s.scope.preAcceptInstance(s.instance)
	c.Assert(err, gocheck.IsNil)

	leaderInstance := copyInstance(s.instance)
	leaderInstance.Dependencies = append(leaderInstance.Dependencies, NewInstanceID())
	leaderInstance.Sequence += 5
	leaderInstance.MaxBallot++

	request := &AcceptRequest{
		Scope: s.scope.name,
		Instance: leaderInstance,
		MissingInstances: []*Instance{},
	}

	response, err := s.scope.HandleAccept(request)
	c.Assert(err, gocheck.IsNil)
	c.Check(response.Accepted, gocheck.Equals, true)

	// check dependencies
	expectedDeps := NewInstanceIDSet(leaderInstance.Dependencies)
	actualDeps := NewInstanceIDSet(s.instance.Dependencies)
	c.Check(len(actualDeps), gocheck.Equals, len(expectedDeps))
	c.Assert(expectedDeps.Equal(actualDeps), gocheck.Equals, true)

	c.Check(leaderInstance.Sequence, gocheck.Equals, s.instance.Sequence)
	c.Check(leaderInstance.Sequence, gocheck.Equals, s.scope.maxSeq)
}

// tests that accepts are handled properly if
// the commit if for an instance the node has
// not been previously seen by this replica
func (s *AcceptReplicaTest) TestNewInstanceSuccess(c *gocheck.C) {
	leaderID := node.NewNodeId()
	leaderInstance := makeInstance(leaderID, s.scope.getCurrentDepsUnsafe())
	leaderInstance.Sequence += 5

	request := &AcceptRequest{
		Scope: s.scope.name,
		Instance: leaderInstance,
		MissingInstances: []*Instance{},
	}

	// sanity checks
	c.Assert(s.scope.instances.ContainsID(leaderInstance.InstanceID), gocheck.Equals, false)

	response, err := s.scope.HandleAccept(request)
	c.Assert(err, gocheck.IsNil)

	c.Assert(s.scope.instances.ContainsID(leaderInstance.InstanceID), gocheck.Equals, true)
	s.instance = s.scope.instances[leaderInstance.InstanceID]

	c.Check(response.Accepted, gocheck.Equals, true)

	// check dependencies
	expectedDeps := NewInstanceIDSet(leaderInstance.Dependencies)
	actualDeps := NewInstanceIDSet(s.instance.Dependencies)
	c.Check(len(actualDeps), gocheck.Equals, len(expectedDeps))
	c.Assert(expectedDeps.Equal(actualDeps), gocheck.Equals, true)

	c.Check(s.instance.Sequence, gocheck.Equals,  leaderInstance.Sequence)
	c.Check(s.scope.maxSeq, gocheck.Equals,  leaderInstance.Sequence)
}

// tests that accept messages fail if an higher
// ballot number has been seen for this message
func (s *AcceptReplicaTest) TestOldBallotFailure(c *gocheck.C) {
	var err error
	err = s.scope.preAcceptInstance(s.instance)
	c.Assert(err, gocheck.IsNil)

	leaderInstance := copyInstance(s.instance)
	leaderInstance.Sequence += 5

	request := &AcceptRequest{
		Scope: s.scope.name,
		Instance: leaderInstance,
		MissingInstances: []*Instance{},
	}

	s.instance.MaxBallot++
	response, err := s.scope.HandleAccept(request)
	c.Assert(err, gocheck.IsNil)

	c.Check(response.Accepted, gocheck.Equals, false)
	c.Check(s.instance.MaxBallot, gocheck.Equals, response.MaxBallot)
}

// tests that handle accept adds any missing instances
// in the missing instances message
func (s *AcceptReplicaTest) TestMissingInstanceSuccess(c *gocheck.C) {
	var err error
	err = s.scope.preAcceptInstance(s.instance)
	c.Assert(err, gocheck.IsNil)

	leaderID := node.NewNodeId()
	missingInstance := makeInstance(leaderID, s.instance.Dependencies)
	leaderInstance := copyInstance(s.instance)
	leaderInstance.Dependencies = append(leaderInstance.Dependencies, missingInstance.InstanceID)
	leaderInstance.Sequence += 5
	leaderInstance.MaxBallot++

	// sanity checks
	c.Check(s.scope.instances.ContainsID(missingInstance.InstanceID), gocheck.Equals, false)

	request := &AcceptRequest{
		Scope: s.scope.name,
		Instance: leaderInstance,
		MissingInstances: []*Instance{missingInstance},
	}

	response, err := s.scope.HandleAccept(request)
	c.Assert(err, gocheck.IsNil)

	c.Check(response.Accepted, gocheck.Equals, true)
	c.Check(s.scope.instances.ContainsID(missingInstance.InstanceID), gocheck.Equals, true)
}

