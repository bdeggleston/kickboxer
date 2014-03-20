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
	instance := s.manager.makeInstance(getBasicInstruction())
	originalBallot := instance.MaxBallot

	// sanity check
	c.Assert(s.manager.instances.Contains(instance), gocheck.Equals, false)
	c.Assert(s.manager.inProgress.Contains(instance), gocheck.Equals, false)
	c.Assert(s.manager.committed.Contains(instance), gocheck.Equals, false)

	seq := s.manager.maxSeq
	err := s.manager.preAcceptInstance(instance, false)
	c.Assert(err, gocheck.IsNil)

	c.Assert(s.manager.instances.Contains(instance), gocheck.Equals, true)
	c.Assert(s.manager.inProgress.Contains(instance), gocheck.Equals, true)
	c.Assert(s.manager.committed.Contains(instance), gocheck.Equals, false)

	c.Check(instance.Sequence, gocheck.Equals, seq + 1)
	c.Check(instance.MaxBallot, gocheck.Equals, originalBallot)
	c.Check(s.manager.maxSeq, gocheck.Equals, seq + 1)
}

func (s *PreAcceptInstanceTest) TestBallotIncrement(c *gocheck.C) {
	instance := s.manager.makeInstance(getBasicInstruction())
	originalBallot := instance.MaxBallot

	err := s.manager.preAcceptInstance(instance, true)
	c.Assert(err, gocheck.IsNil)

	c.Check(instance.MaxBallot, gocheck.Equals, originalBallot + 1)
}

func (s *PreAcceptInstanceTest) TestHigherStatusFailure(c *gocheck.C) {
	var err error
	instance := s.manager.makeInstance(getBasicInstruction())
	err = s.manager.acceptInstance(instance, false)
	c.Assert(err, gocheck.IsNil)


	// sanity check
	c.Assert(s.manager.instances.Contains(instance), gocheck.Equals, true)
	c.Assert(s.manager.inProgress.Contains(instance), gocheck.Equals, true)
	c.Assert(s.manager.committed.Contains(instance), gocheck.Equals, false)

	err = s.manager.preAcceptInstance(instance, false)
	c.Assert(err, gocheck.NotNil)
	c.Assert(err, gocheck.FitsTypeOf, InvalidStatusUpdateError{})

	c.Check(instance.Status, gocheck.Equals, INSTANCE_ACCEPTED)
}

// if an instance is being preaccepted twice
// which is possible if there's an explicit
// prepare, it should copy some attributes,
// (noop), and not overwrite any existing
// instances references in the manager's containers
func (s *PreAcceptInstanceTest) TestRepeatPreaccept(c *gocheck.C ) {
	var err error
	instance := s.manager.makeInstance(getBasicInstruction())
	repeat := copyInstance(instance)

	err = s.manager.preAcceptInstance(instance, false)
	c.Assert(err, gocheck.IsNil)

	err = s.manager.preAcceptInstance(repeat, false)
	c.Assert(err, gocheck.IsNil)
 	c.Assert(s.manager.instances.Get(instance.InstanceID), gocheck.Equals, instance)
	c.Assert(s.manager.instances.Get(instance.InstanceID), gocheck.Not(gocheck.Equals), repeat)
	c.Assert(s.manager.inProgress.Get(instance.InstanceID), gocheck.Equals, instance)
	c.Assert(s.manager.inProgress.Get(instance.InstanceID), gocheck.Not(gocheck.Equals), repeat)
}

// tests that the noop flag is recognized when
// preaccepting new instances
func (s *PreAcceptInstanceTest) TestNewNoopPreaccept(c *gocheck.C) {
	var err error
	instance := s.manager.makeInstance(getBasicInstruction())
	instance.Noop = true

	err = s.manager.preAcceptInstance(instance, false)
	c.Assert(err, gocheck.IsNil)

	c.Assert(s.manager.instances.Get(instance.InstanceID).Noop, gocheck.Equals, true)
}

// tests that the noop flag is recognized when
// preaccepting previously seen instances
func (s *PreAcceptInstanceTest) TestOldNoopPreaccept(c *gocheck.C) {
	var err error
	instance := s.manager.makeInstance(getBasicInstruction())
	repeat := copyInstance(instance)

	err = s.manager.preAcceptInstance(instance, false)
	c.Assert(err, gocheck.IsNil)
	c.Assert(instance.Noop, gocheck.Equals, false)

	repeat.Noop = true
	err = s.manager.preAcceptInstance(repeat, false)
	c.Assert(err, gocheck.IsNil)
	c.Assert(instance.Noop, gocheck.Equals, true)
}

type PreAcceptLeaderTest struct {
	baseReplicaTest
	instance *Instance
}

func (s *PreAcceptLeaderTest) SetUpTest(c *gocheck.C) {
	s.baseReplicaTest.SetUpTest(c)

	s.instance = s.manager.makeInstance(getBasicInstruction())
	err := s.manager.preAcceptInstance(s.instance, false)
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

	responses, err := s.manager.sendPreAccept(s.instance, transformMockNodeArray(s.replicas))
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
	// TODO: mock timeout
	oldPreAcceptTimeout := PREACCEPT_TIMEOUT
	PREACCEPT_TIMEOUT = uint64(50)
	defer func(){ PREACCEPT_TIMEOUT = oldPreAcceptTimeout }()
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

	responses, err := s.manager.sendPreAccept(s.instance, transformMockNodeArray(s.replicas))
	c.Assert(err, gocheck.NotNil)
	c.Assert(err, gocheck.FitsTypeOf, TimeoutError{})
	c.Assert(responses, gocheck.IsNil)
}

// check that a ballot error is returned if the remote instance
// rejects the message
func (s *PreAcceptLeaderTest) TestSendBallotFailure(c *gocheck.C) {
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		newInst := copyInstance(s.instance)
		return &PreAcceptResponse{
			Accepted:         true,
			MaxBallot:        newInst.MaxBallot,
			Instance:         newInst,
			MissingInstances: []*Instance{},
		}, nil
	}
	rejectFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		newInst := copyInstance(s.instance)
		return &PreAcceptResponse{
			Accepted:         false,
			MaxBallot:        newInst.MaxBallot + 1,
			Instance:         newInst,
			MissingInstances: []*Instance{},
		}, nil
	}

	for i, replica := range s.replicas {
		if i == 0 {
			replica.messageHandler = responseFunc
		} else {
			replica.messageHandler = rejectFunc
		}
	}

	responses, err := s.manager.sendPreAccept(s.instance, transformMockNodeArray(s.replicas))
	c.Assert(err, gocheck.NotNil)
	c.Assert(err, gocheck.FitsTypeOf, BallotError{})
	c.Assert(responses, gocheck.IsNil)
}

func (s *PreAcceptLeaderTest) TestMergeAttributes(c *gocheck.C) {
	// setup local instance seq & deps
	for i := 0; i < 4; i++ {
		s.instance.Dependencies = append(s.instance.Dependencies, NewInstanceID())
	}
	s.instance.Sequence = 3
	expected := NewInstanceIDSet(s.instance.Dependencies)

	// setup remote instance seq & deps
	remoteInstance1, err := s.instance.Copy()
	c.Assert(err, gocheck.IsNil)
	remoteInstance1.Dependencies = s.instance.Dependencies[1:]
	remoteInstance1.Dependencies = append(remoteInstance1.Dependencies, NewInstanceID())
	remoteInstance1.Sequence++
	expected.Add(remoteInstance1.Dependencies...)

	remoteInstance2, err := s.instance.Copy()
	c.Assert(err, gocheck.IsNil)
	remoteInstance2.Dependencies = s.instance.Dependencies[2:]
	remoteInstance2.Dependencies = append(remoteInstance2.Dependencies, NewInstanceID())
	remoteInstance2.Dependencies = append(remoteInstance2.Dependencies, NewInstanceID())
	remoteInstance2.Sequence++
	expected.Add(remoteInstance2.Dependencies...)

	// sanity checks
	c.Assert(len(s.instance.Dependencies), gocheck.Equals, 4)
	c.Assert(len(remoteInstance1.Dependencies), gocheck.Equals, 4)
	c.Assert(s.instance.Sequence, gocheck.Equals, uint64(3))
	c.Assert(remoteInstance1.Sequence, gocheck.Equals, uint64(4))

	//
	responses := []*PreAcceptResponse{&PreAcceptResponse{
		Accepted:         true,
		MaxBallot:        remoteInstance1.MaxBallot,
		Instance:         remoteInstance1,
		MissingInstances: []*Instance{},
	}, &PreAcceptResponse{
		Accepted:         true,
		MaxBallot:        remoteInstance2.MaxBallot,
		Instance:         remoteInstance2,
		MissingInstances: []*Instance{},
	}}

	changes, err := s.manager.mergePreAcceptAttributes(s.instance, responses)
	c.Assert(err, gocheck.IsNil)
	c.Check(changes, gocheck.Equals, true)

	actual := NewInstanceIDSet(s.instance.Dependencies)
	c.Check(actual, gocheck.DeepEquals, expected)
	c.Check(s.instance.Sequence, gocheck.Equals, remoteInstance2.Sequence)
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
	changes, err := s.manager.mergePreAcceptAttributes(s.instance, responses)

	c.Assert(err, gocheck.IsNil)
	c.Check(changes, gocheck.Equals, false)
	c.Assert(len(s.instance.Dependencies), gocheck.Equals, 4)

	actual := NewInstanceIDSet(s.instance.Dependencies)
	c.Check(s.instance.Sequence, gocheck.Equals, uint64(3))
	c.Check(expected.Equal(actual), gocheck.Equals, true)
}

// tests that the accept messages sent out have the same ballot
// as the local instance
func (s *PreAcceptLeaderTest) TestPreAcceptMessageBallotIsUpToDate(c *gocheck.C) {
	var sentBallot uint32
	// all replicas agree
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		request := m.(*PreAcceptRequest)
		sentBallot = request.Instance.MaxBallot
		return &PreAcceptResponse{
			Accepted:         true,
			MaxBallot:        request.Instance.MaxBallot,
			Instance:         request.Instance,
			MissingInstances: []*Instance{},
		}, nil
	}

	for _, replica := range s.replicas {
		replica.messageHandler = responseFunc
	}

	duplicateInstance, err := s.instance.Copy()
	c.Assert(err, gocheck.IsNil)

	expectedBallot := duplicateInstance.MaxBallot + 1
	_, err = s.manager.preAcceptPhase(duplicateInstance)
	c.Assert(err, gocheck.IsNil)
	c.Assert(sentBallot, gocheck.Equals, expectedBallot)
}

type PreAcceptReplicaTest struct {
	baseScopeTest
}

var _ = gocheck.Suite(&PreAcceptReplicaTest{})

func (s *PreAcceptReplicaTest) SetUpTest(c *gocheck.C) {
	s.baseScopeTest.SetUpTest(c)
	setupDeps(s.manager)
}

// tests that the dependency match flag is set
// if the seq and deps matched
func (s *PreAcceptReplicaTest) TestHandleIdenticalAttrs(c *gocheck.C) {
	s.manager.maxSeq = 3

	instructions := getBasicInstruction()
	instance := &Instance{
		InstanceID:   NewInstanceID(),
		LeaderID:     node.NewNodeId(),
		Commands:     instructions,
		Dependencies: s.manager.getInstructionDeps(instructions),
		Sequence:     s.manager.maxSeq + 1,
		Status:       INSTANCE_PREACCEPTED,
	}
	request := &PreAcceptRequest{
		Instance: instance,
	}

	// process the preaccept message
	response, err := s.manager.HandlePreAccept(request)
	c.Assert(err, gocheck.IsNil)
	c.Assert(response, gocheck.NotNil)
	c.Check(response.Accepted, gocheck.Equals, true)

	// check dependencies
	localInstance := s.manager.instances.Get(instance.InstanceID)
	expectedDeps := NewInstanceIDSet(instance.Dependencies)
	actualDeps := NewInstanceIDSet(localInstance.Dependencies)

	c.Assert(expectedDeps.Equal(actualDeps), gocheck.Equals, true)
	c.Check(localInstance.Sequence, gocheck.Equals, uint64(4))
	c.Check(localInstance.DependencyMatch, gocheck.Equals, true)
	c.Check(len(response.MissingInstances), gocheck.Equals, 0)
}

// tests that the replica updates the sequence and
// dependencies if it disagrees with the leader
func (s *PreAcceptReplicaTest) TestHandleDifferentAttrs(c *gocheck.C) {
	s.manager.maxSeq = 3

	instructions := getBasicInstruction()
	replicaDeps := s.manager.getInstructionDeps(instructions)
	leaderDeps := s.manager.getInstructionDeps(instructions)
	missingDep := leaderDeps[0]
	extraDep := NewInstanceID()
	leaderDeps[0] = extraDep
	instance := &Instance{
		InstanceID:   NewInstanceID(),
		LeaderID:     node.NewNodeId(),
		Commands:     instructions,
		Dependencies: leaderDeps,
		Sequence:     3,
		Status:       INSTANCE_PREACCEPTED,
	}
	request := &PreAcceptRequest{
		Instance: instance,
	}

	s.manager.instances.Add(&Instance{InstanceID: missingDep})

	// process the preaccept message
	response, err := s.manager.HandlePreAccept(request)
	c.Assert(err, gocheck.IsNil)
	c.Assert(response, gocheck.NotNil)
	c.Check(response.Accepted, gocheck.Equals, true)

	responseInst := response.Instance
	expectedDeps := NewInstanceIDSet(replicaDeps)

	actualDeps := NewInstanceIDSet(responseInst.Dependencies)
	c.Check(len(actualDeps), gocheck.Equals, len(expectedDeps))
	c.Assert(expectedDeps.Equal(actualDeps), gocheck.Equals, true)

	c.Check(responseInst.Sequence, gocheck.Equals, uint64(4))
	c.Check(responseInst.DependencyMatch, gocheck.Equals, false)

	// check that handle pre-accept returns any missing
	// instance dependencies that the leader didn't include
	c.Assert(len(response.MissingInstances), gocheck.Equals, 1)
	c.Check(response.MissingInstances[0].InstanceID, gocheck.Equals, missingDep)
}

// tests that new attributes are returned
func (s *PreAcceptReplicaTest) TestHandleNewAttrs(c *gocheck.C) {
	s.manager.maxSeq = 3

	instance := &Instance{
		InstanceID:   NewInstanceID(),
		LeaderID:     node.NewNodeId(),
		Commands:     getBasicInstruction(),
		Dependencies: []InstanceID{},
		Sequence:     s.manager.maxSeq + 1,
		Status:       INSTANCE_PREACCEPTED,
	}
	replicaDeps := s.manager.getInstanceDeps(instance)
	c.Assert(len(replicaDeps) > 0, gocheck.Equals, true)
	request := &PreAcceptRequest{
		Instance: instance,
	}

	// process the preaccept message
	response, err := s.manager.HandlePreAccept(request)
	c.Assert(err, gocheck.IsNil)
	c.Assert(response, gocheck.NotNil)
	c.Check(response.Accepted, gocheck.Equals, true)

	// check dependencies
	localInstance := s.manager.instances.Get(instance.InstanceID)
	c.Assert(response.Instance, gocheck.Not(gocheck.Equals), localInstance)

	expectedDeps := NewInstanceIDSet(replicaDeps)
	actualDeps := NewInstanceIDSet(response.Instance.Dependencies)
	c.Assert(expectedDeps.Equal(actualDeps), gocheck.Equals, true)
	c.Check(len(response.MissingInstances), gocheck.Equals, len(replicaDeps))
}

// tests that new attributes are returned
func (s *PreAcceptReplicaTest) TestHandleBallotFailure(c *gocheck.C) {
	instance := s.manager.makeInstance(getBasicInstruction())
	instance.MaxBallot = 5
	s.manager.preAcceptInstance(instance, false)

	c.Assert(instance.MaxBallot, gocheck.Equals, uint32(5))

	instanceCopy, err := instance.Copy()
	c.Assert(err, gocheck.IsNil)
	instanceCopy.MaxBallot--
	request := &PreAcceptRequest{
		Instance: instanceCopy,
	}

	// process the preaccept message
	response, err := s.manager.HandlePreAccept(request)
	c.Assert(err, gocheck.IsNil)
	c.Assert(response, gocheck.NotNil)
	c.Check(response.Accepted, gocheck.Equals, false)

}
