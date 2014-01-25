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

type PreAcceptInstanceTest struct {
	baseScopeTest
}

var _ = gocheck.Suite(&PreAcceptInstanceTest{})

func (s *PreAcceptInstanceTest) TestSuccessCase(c *gocheck.C) {
	instance := s.scope.makeInstance(getBasicInstruction())

	// sanity check
	c.Assert(s.scope.instances.Contains(instance), gocheck.Equals, false)
	c.Assert(s.scope.inProgress.Contains(instance), gocheck.Equals, false)
	c.Assert(s.scope.committed.Contains(instance), gocheck.Equals, false)

	seq := s.scope.maxSeq
	err := s.scope.preAcceptInstance(instance)
	c.Assert(err, gocheck.IsNil)

	c.Assert(s.scope.instances.Contains(instance), gocheck.Equals, true)
	c.Assert(s.scope.inProgress.Contains(instance), gocheck.Equals, true)
	c.Assert(s.scope.committed.Contains(instance), gocheck.Equals, false)

	c.Check(instance.Sequence, gocheck.Equals, seq + 1)
	c.Check(s.scope.maxSeq, gocheck.Equals, seq + 1)
}

func (s *PreAcceptInstanceTest) TestHigherStatusFailure(c *gocheck.C) {
	var err error
	instance := s.scope.makeInstance(getBasicInstruction())
	err = s.scope.acceptInstance(instance)
	c.Assert(err, gocheck.IsNil)


	// sanity check
	c.Assert(s.scope.instances.Contains(instance), gocheck.Equals, true)
	c.Assert(s.scope.inProgress.Contains(instance), gocheck.Equals, true)
	c.Assert(s.scope.committed.Contains(instance), gocheck.Equals, false)

	err = s.scope.preAcceptInstance(instance)
	c.Assert(err, gocheck.NotNil)
	c.Assert(err, gocheck.FitsTypeOf, InvalidStatusUpdateError{})

	c.Check(instance.Status, gocheck.Equals, INSTANCE_ACCEPTED)
}

// if an instance is being preaccepted twice
// which is possible if there's an explicit
// prepare, it should copy some attributes,
// (noop), and not overwrite any existing
// instances references in the scope's containers
func (s *PreAcceptInstanceTest) TestRepeatPreaccept(c *gocheck.C ) {
	var err error
	instance := s.scope.makeInstance(getBasicInstruction())
	repeat := copyInstance(instance)

	err = s.scope.preAcceptInstance(instance)
	c.Assert(err, gocheck.IsNil)

	err = s.scope.preAcceptInstance(repeat)
	c.Assert(err, gocheck.IsNil)
 	c.Assert(s.scope.instances[instance.InstanceID], gocheck.Equals, instance)
	c.Assert(s.scope.inProgress[instance.InstanceID], gocheck.Equals, instance)
}

// tests that the noop flag is recognized when
// preaccepting new instances
func (s *PreAcceptInstanceTest) TestNewNoopPreaccept(c *gocheck.C) {
	var err error
	instance := s.scope.makeInstance(getBasicInstruction())
	instance.Noop = true

	err = s.scope.preAcceptInstance(instance)
	c.Assert(err, gocheck.IsNil)

	c.Assert(s.scope.instances[instance.InstanceID].Noop, gocheck.Equals, true)
}

// tests that the noop flag is recognized when
// preaccepting previously seen instances
func (s *PreAcceptInstanceTest) TestOldNoopPreaccept(c *gocheck.C) {
	var err error
	instance := s.scope.makeInstance(getBasicInstruction())
	repeat := copyInstance(instance)

	err = s.scope.preAcceptInstance(instance)
	c.Assert(err, gocheck.IsNil)
	c.Assert(instance.Noop, gocheck.Equals, false)

	repeat.Noop = true
	err = s.scope.preAcceptInstance(repeat)
	c.Assert(err, gocheck.IsNil)
	c.Assert(instance.Noop, gocheck.Equals, true)
}

type PreAcceptLeaderTest struct {
	baseReplicaTest
	instance *Instance
}

func (s *PreAcceptLeaderTest) SetUpTest(c *gocheck.C) {
	s.baseReplicaTest.SetUpTest(c)

	s.instance = s.scope.makeInstance(getBasicInstruction())
	err := s.scope.preAcceptInstance(s.instance)
	c.Assert(err, gocheck.IsNil)
}

var _ = gocheck.Suite(&PreAcceptLeaderTest{})

// tests all replicas returning results
func (s *PreAcceptLeaderTest) TestSendSuccessCase(c *gocheck.C) {
	// all replicas agree
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		newInst := copyInstance(s.instance)
		return &PreAcceptResponse{
			Accepted:         true,
			MaxBallot:        newInst.MaxBallot,
			Instance:         newInst,
			MissingInstances: []*Instance{},
		}, nil
	}

	for _, replica := range s.replicas {
		replica.messageHandler = responseFunc
	}

	responses, err := s.scope.sendPreAccept(s.instance, transformMockNodeArray(s.replicas))
	c.Assert(err, gocheck.IsNil)
	c.Log(len(s.replicas))
	c.Log(len(responses))
	c.Assert(len(responses) < s.quorumSize() - 1, gocheck.Equals, false)  // less than quorum received

	// test that the nodes received the correct message
	for _, replica := range s.replicas {
		c.Assert(len(replica.sentMessages), gocheck.Equals, 1)
		c.Assert(replica.sentMessages[0], gocheck.FitsTypeOf, &PreAcceptRequest{})
	}
}

func (s *PreAcceptLeaderTest) TestSendQuorumFailure(c *gocheck.C) {
	// all replicas agree
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		newInst := copyInstance(s.instance)
		return &PreAcceptResponse{
			Accepted:         true,
			MaxBallot:        newInst.MaxBallot,
			Instance:         newInst,
			MissingInstances: []*Instance{},
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

	responses, err := s.scope.sendPreAccept(s.instance, transformMockNodeArray(s.replicas))
	c.Assert(err, gocheck.NotNil)
	c.Assert(err, gocheck.FitsTypeOf, TimeoutError{})
	c.Assert(responses, gocheck.IsNil)
}

func (s *PreAcceptLeaderTest) TestSendBallotFailure(c *gocheck.C) {
	// TODO: figure out what to do in this situation
	// the only way this would happen if is the command
	// was taken over by another replica, in which case,
	// should we just wait for the other leader to
	// execute it?
	c.Skip("figure out the expected behavior")
}

func (s *PreAcceptLeaderTest) TestMergeAttributes(c *gocheck.C) {
	// setup local instance seq & deps
	for i := 0; i < 4; i++ {
		s.instance.Dependencies = append(s.instance.Dependencies, NewInstanceID())
	}
	s.instance.Sequence = 3
	expected := NewInstanceIDSet(s.instance.Dependencies)

	// setup remote instance seq & deps
	remoteInstance := copyInstance(s.instance)
	remoteInstance.Dependencies = s.instance.Dependencies[1:]
	remoteInstance.Dependencies = append(remoteInstance.Dependencies, NewInstanceID())
	remoteInstance.Sequence++
	expected.Add(remoteInstance.Dependencies...)

	// sanity checks
	c.Assert(len(s.instance.Dependencies), gocheck.Equals, 4)
	c.Assert(len(remoteInstance.Dependencies), gocheck.Equals, 4)
	c.Assert(s.instance.Sequence, gocheck.Equals, uint64(3))
	c.Assert(remoteInstance.Sequence, gocheck.Equals, uint64(4))

	//
	responses := []*PreAcceptResponse{&PreAcceptResponse{
		Accepted:         true,
		MaxBallot:        remoteInstance.MaxBallot,
		Instance:         remoteInstance,
		MissingInstances: []*Instance{},
	}}
	changes, err := s.scope.mergePreAcceptAttributes(s.instance, responses)
	c.Assert(err, gocheck.IsNil)
	c.Check(changes, gocheck.Equals, true)
	c.Assert(len(s.instance.Dependencies), gocheck.Equals, 5)

	actual := NewInstanceIDSet(s.instance.Dependencies)
	c.Check(s.instance.Sequence, gocheck.Equals, uint64(4))
	c.Check(expected.Equal(actual), gocheck.Equals, true)
}

func (s *PreAcceptLeaderTest) TestMergeAttributesNoChanges(c *gocheck.C) {
	// setup local instance seq & deps
	for i := 0; i < 4; i++ {
		s.instance.Dependencies = append(s.instance.Dependencies, NewInstanceID())
	}
	s.instance.Sequence = 3
	expected := NewInstanceIDSet(s.instance.Dependencies)

	// setup remote instance seq & deps
	remoteInstance := copyInstance(s.instance)

	// sanity checks
	c.Assert(len(s.instance.Dependencies), gocheck.Equals, 4)
	c.Assert(len(remoteInstance.Dependencies), gocheck.Equals, 4)
	c.Assert(s.instance.Sequence, gocheck.Equals, uint64(3))
	c.Assert(remoteInstance.Sequence, gocheck.Equals, uint64(3))

	responses := []*PreAcceptResponse{&PreAcceptResponse{
		Accepted:         true,
		MaxBallot:        remoteInstance.MaxBallot,
		Instance:         remoteInstance,
		MissingInstances: []*Instance{},
	}}
	changes, err := s.scope.mergePreAcceptAttributes(s.instance, responses)

	c.Assert(err, gocheck.IsNil)
	c.Check(changes, gocheck.Equals, false)
	c.Assert(len(s.instance.Dependencies), gocheck.Equals, 4)

	actual := NewInstanceIDSet(s.instance.Dependencies)
	c.Check(s.instance.Sequence, gocheck.Equals, uint64(3))
	c.Check(expected.Equal(actual), gocheck.Equals, true)
}

type PreAcceptReplicaTest struct {
	baseScopeTest
}

var _ = gocheck.Suite(&PreAcceptReplicaTest{})

func (s *PreAcceptReplicaTest) SetUpTest(c *gocheck.C) {
	s.baseScopeTest.SetUpTest(c)
	setupDeps(s.scope)
}

// tests that the dependency match flag is set
// if the seq and deps matched
func (s *PreAcceptReplicaTest) TestHandleIdenticalAttrs(c *gocheck.C) {
	s.scope.maxSeq = 3

	instance := &Instance{
		InstanceID:   NewInstanceID(),
		LeaderID:     node.NewNodeId(),
		Commands:     getBasicInstruction(),
		Dependencies: s.scope.getCurrentDepsUnsafe(),
		Sequence:     s.scope.maxSeq + 1,
		Status:       INSTANCE_PREACCEPTED,
	}
	request := &PreAcceptRequest{
		Scope:    s.scope.name,
		Instance: instance,
	}

	// process the preaccept message
	response, err := s.scope.HandlePreAccept(request)
	c.Assert(err, gocheck.IsNil)
	c.Assert(response, gocheck.NotNil)
	c.Check(response.Accepted, gocheck.Equals, true)

	// check dependencies
	localInstance := s.scope.instances[instance.InstanceID]
	expectedDeps := NewInstanceIDSet(instance.Dependencies)
	actualDeps := NewInstanceIDSet(localInstance.Dependencies)

	c.Assert(expectedDeps.Equal(actualDeps), gocheck.Equals, true)
	c.Check(localInstance.Sequence, gocheck.Equals, uint64(4))
	c.Check(localInstance.dependencyMatch, gocheck.Equals, true)
	c.Check(len(response.MissingInstances), gocheck.Equals, 0)
}

// tests that the replica updates the sequence and
// dependencies if it disagrees with the leader
func (s *PreAcceptReplicaTest) TestHandleDifferentAttrs(c *gocheck.C) {
	s.scope.maxSeq = 3

	replicaDeps := s.scope.getCurrentDepsUnsafe()
	leaderDeps := s.scope.getCurrentDepsUnsafe()
	missingDep := leaderDeps[0]
	extraDep := NewInstanceID()
	leaderDeps[0] = extraDep
	instance := &Instance{
		InstanceID:   NewInstanceID(),
		LeaderID:     node.NewNodeId(),
		Commands:     getBasicInstruction(),
		Dependencies: leaderDeps,
		Sequence:     3,
		Status:       INSTANCE_PREACCEPTED,
	}
	request := &PreAcceptRequest{
		Scope:    s.scope.name,
		Instance: instance,
	}

	s.scope.instances[missingDep] = &Instance{InstanceID: missingDep}

	// process the preaccept message
	response, err := s.scope.HandlePreAccept(request)
	c.Assert(err, gocheck.IsNil)
	c.Assert(response, gocheck.NotNil)
	c.Check(response.Accepted, gocheck.Equals, true)

	responseInst := response.Instance
	expectedDeps := NewInstanceIDSet(replicaDeps)

	actualDeps := NewInstanceIDSet(responseInst.Dependencies)
	c.Check(len(actualDeps), gocheck.Equals, len(expectedDeps))
	c.Assert(expectedDeps.Equal(actualDeps), gocheck.Equals, true)

	c.Check(responseInst.Sequence, gocheck.Equals, uint64(4))
	c.Check(responseInst.dependencyMatch, gocheck.Equals, false)

	// check that handle pre-accept returns any missing
	// instance dependencies that the leader didn't include
	c.Assert(len(response.MissingInstances), gocheck.Equals, 1)
	c.Check(response.MissingInstances[0].InstanceID, gocheck.Equals, missingDep)
}

