package consensus

import (
	"time"
)

import (
	"launchpad.net/gocheck"
)

import (
	"message"
	"node"
	"runtime"
	"store"
)

type CommitInstanceTest struct {
	baseManagerTest
}

var _ = gocheck.Suite(&CommitInstanceTest{})

// tests that an instance is marked as committed,
// added to the committed set, removed from the
// inProgress set, and persisted if it hasn't
// already been executed
func (s *CommitInstanceTest) TestExistingSuccessCase(c *gocheck.C) {
	replicaInstance := makeInstance(s.manager.GetLocalID(), makeDependencies(4))
	s.manager.maxSeq = 3
	replicaInstance.Sequence = s.manager.maxSeq
	s.manager.acceptInstance(replicaInstance, false)
	originalBallot := replicaInstance.MaxBallot

	leaderInstance, _ := replicaInstance.Copy()
	leaderInstance.Sequence++
	leaderInstance.Dependencies = append(leaderInstance.Dependencies, NewInstanceID())

	// sanity checks
	c.Assert(s.manager.inProgress.Contains(leaderInstance), gocheck.Equals, true)
	c.Assert(s.manager.committed.Contains(leaderInstance), gocheck.Equals, false)

	c.Assert(len(replicaInstance.Dependencies), gocheck.Equals, 4)
	c.Assert(replicaInstance.Sequence, gocheck.Equals, uint64(3))
	c.Assert(s.manager.maxSeq, gocheck.Equals, uint64(3))
	c.Check(replicaInstance.executeTimeout.IsZero(), gocheck.Equals, true)

	oldStatCommitCount := s.manager.stats.(*mockStatter).counters["commit.instance.count"]
	err := s.manager.commitInstance(leaderInstance, false)
	c.Assert(err, gocheck.IsNil)

	// test bookkeeping
	c.Assert(s.manager.inProgress.Contains(leaderInstance), gocheck.Equals, false)
	c.Assert(s.manager.committed.Contains(leaderInstance), gocheck.Equals, true)

	c.Check(replicaInstance.Status, gocheck.Equals, INSTANCE_COMMITTED)
	c.Check(replicaInstance.MaxBallot, gocheck.Equals, originalBallot)
	c.Check(len(replicaInstance.Dependencies), gocheck.Equals, 5)
	c.Check(replicaInstance.Sequence, gocheck.Equals, uint64(4))
	c.Check(s.manager.maxSeq, gocheck.Equals, uint64(4))
	c.Check(s.manager.stats.(*mockStatter).counters["commit.instance.count"], gocheck.Equals, oldStatCommitCount + 1)
	c.Check(replicaInstance.executeTimeout.IsZero(), gocheck.Equals, false)
	c.Check(replicaInstance.executeTimeout.After(time.Now()), gocheck.Equals, true)
}

func (s *CommitInstanceTest) TestBallotIncrement(c *gocheck.C) {
	instance := makeInstance(s.manager.GetLocalID(), makeDependencies(4))
	originalBallot := instance.MaxBallot
	err := s.manager.commitInstance(instance, true)
	c.Assert(err, gocheck.IsNil)

	c.Check(instance.MaxBallot, gocheck.Equals, originalBallot + 1)
}
// tests that an instance is marked as committed,
// added to the instances and committed set, and
// persisted if the instance hasn't been seen before
func (s *CommitInstanceTest) TestNewSuccessCase(c *gocheck.C) {
	s.manager.maxSeq = 3

	leaderInstance := makeInstance(node.NewNodeId(), makeDependencies(4))
	leaderInstance.Sequence = s.manager.maxSeq + 2

	// sanity checks
	c.Assert(s.manager.instances.Contains(leaderInstance), gocheck.Equals, false)
	c.Assert(s.manager.inProgress.Contains(leaderInstance), gocheck.Equals, false)
	c.Assert(s.manager.committed.Contains(leaderInstance), gocheck.Equals, false)


	err := s.manager.commitInstance(leaderInstance, false)
	c.Assert(err, gocheck.IsNil)

	c.Assert(s.manager.instances.Contains(leaderInstance), gocheck.Equals, true)
	c.Assert(s.manager.inProgress.Contains(leaderInstance), gocheck.Equals, false)
	c.Assert(s.manager.committed.Contains(leaderInstance), gocheck.Equals, true)

	replicaInstance := s.manager.instances.Get(leaderInstance.InstanceID)

	c.Check(replicaInstance.Status, gocheck.Equals,  INSTANCE_COMMITTED)
	c.Check(leaderInstance.Status, gocheck.Equals, INSTANCE_COMMITTED)
	c.Check(len(replicaInstance.Dependencies), gocheck.Equals, 4)
	c.Check(replicaInstance.Sequence, gocheck.Equals, uint64(5))
	c.Check(s.manager.maxSeq, gocheck.Equals, uint64(5))
}

// tests that an instance is not marked as committed,
// or added to the committed set if it already has
// been executed
func (s *CommitInstanceTest) TestCommitExecutedFailure(c *gocheck.C) {
	replicaInstance := makeInstance(s.manager.GetLocalID(), makeDependencies(4))
	s.manager.maxSeq = 3
	replicaInstance.Sequence = s.manager.maxSeq
	replicaInstance.Status = INSTANCE_EXECUTED

	s.manager.instances.Add(replicaInstance)
	s.manager.executed = append(s.manager.executed, replicaInstance.InstanceID)

	leaderInstance, _ := replicaInstance.Copy()
	leaderInstance.Status = INSTANCE_ACCEPTED

	// sanity checks
	c.Assert(s.manager.committed.Contains(leaderInstance), gocheck.Equals, false)
	c.Assert(s.manager.inProgress.Contains(leaderInstance), gocheck.Equals, false)
	c.Assert(s.manager.executed[len(s.manager.executed) - 1] == replicaInstance.InstanceID, gocheck.Equals, true)

	err := s.manager.commitInstance(leaderInstance, false)
	c.Assert(err, gocheck.FitsTypeOf, InvalidStatusUpdateError{})

	// check set memberships haven't changed
	c.Assert(s.manager.committed.Contains(leaderInstance), gocheck.Equals, false)
	c.Assert(s.manager.inProgress.Contains(leaderInstance), gocheck.Equals, false)
	c.Assert(s.manager.executed[len(s.manager.executed) - 1] == replicaInstance.InstanceID, gocheck.Equals, true)
	c.Check(replicaInstance.Status, gocheck.Equals, INSTANCE_EXECUTED)
}

// if an instance is being committed twice
// which is possible if there's an explicit
// prepare, it should copy some attributes,
// (noop), and not overwrite any existing
// instances references in the manager's containers
func (s *CommitInstanceTest) TestRepeatCommit(c *gocheck.C ) {
	var err error
	instance := s.manager.makeInstance(getBasicInstruction())
	repeat, _ := instance.Copy()

	err = s.manager.commitInstance(instance, false)
	c.Assert(err, gocheck.IsNil)

	err = s.manager.commitInstance(repeat, false)
	c.Assert(err, gocheck.IsNil)
	c.Assert(s.manager.instances.Get(instance.InstanceID), gocheck.Equals, instance)
	c.Assert(s.manager.instances.Get(instance.InstanceID), gocheck.Not(gocheck.Equals), repeat)
	c.Assert(s.manager.committed.Get(instance.InstanceID), gocheck.Equals, instance)
	c.Assert(s.manager.committed.Get(instance.InstanceID), gocheck.Not(gocheck.Equals), repeat)
}

// tests that instances with a commitNotify Cond object
// calls broadcast when the instance is committed
func (s *CommitInstanceTest) TestCommitInstanceBroadcast(c *gocheck.C) {

}

func (s *CommitInstanceTest) TestCommitNoop(c *gocheck.C) {

}


/** leader **/
type CommitLeaderTest struct {
	baseReplicaTest
	instance *Instance
	oldAcceptTimeout uint64
}

var _ = gocheck.Suite(&CommitLeaderTest{})

func (s *CommitLeaderTest) SetUpTest(c *gocheck.C) {
	s.baseReplicaTest.SetUpTest(c)
	s.instance = s.manager.makeInstance(getBasicInstruction())
	var err error

	err = s.manager.preAcceptInstance(s.instance, false)
	c.Assert(err, gocheck.IsNil)
	err = s.manager.acceptInstance(s.instance, false)
	c.Assert(err, gocheck.IsNil)
	err = s.manager.commitInstance(s.instance, false)
	c.Assert(err, gocheck.IsNil)
}

// tests that calling sendCommit sends commit requests
// to the other replicas
func (s *CommitLeaderTest) TestSendSuccess(c *gocheck.C) {
	err := s.manager.sendCommit(s.instance, transformMockNodeArray(s.replicas))
	c.Assert(err, gocheck.IsNil)

	// all replicas agree
	responseWaitChan := make(chan bool, len(s.replicas))
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		responseWaitChan <- true
		return &CommitResponse{}, nil
	}

	for _, replica := range s.replicas {
		replica.messageHandler = responseFunc
	}

	// wait for all of the nodes to receive their messages
	for i:=0;i<len(s.replicas);i++ {
		<-responseWaitChan
	}

	// test that the nodes received the correct message
	for _, replica := range s.replicas {
		c.Assert(len(replica.sentMessages), gocheck.Equals, 1)
		c.Assert(replica.sentMessages[0], gocheck.FitsTypeOf, &CommitRequest{})
	}
}

// tests that the accept messages sent out have the same ballot
// as the local instance
func (s *CommitLeaderTest) TestCommitMessageBallotIsUpToDate(c *gocheck.C) {
	var sentBallot uint32
	// all replicas agree
	responseWaitChan := make(chan bool, len(s.replicas))
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		request := m.(*CommitRequest)
		sentBallot = request.Instance.MaxBallot
		responseWaitChan <- true
		return &CommitResponse{}, nil
	}

	for _, replica := range s.replicas {
		replica.messageHandler = responseFunc
	}

	duplicateInstance, err := s.instance.Copy()
	c.Assert(err, gocheck.IsNil)

	expectedBallot := duplicateInstance.MaxBallot + 1
	err = s.manager.commitPhase(duplicateInstance)

	// wait for all of the nodes to receive their messages
	for i:=0;i<len(s.replicas);i++ {
		<-responseWaitChan
	}

	c.Assert(err, gocheck.IsNil)
	c.Assert(sentBallot, gocheck.Equals, expectedBallot)
}


type CommitReplicaTest struct {
	baseManagerTest
}

var _ = gocheck.Suite(&CommitReplicaTest{})

// tests that an instance is marked as committed when
// a commit request is recived
func (s *CommitReplicaTest) TestHandleSuccess(c *gocheck.C) {
	instance := s.manager.makeInstance(getBasicInstruction())
	err := s.manager.acceptInstance(instance, false)
	c.Assert(err, gocheck.IsNil)

	// sanity check
	c.Assert(instance.Status, gocheck.Equals, INSTANCE_ACCEPTED)

	request := &CommitRequest{
		Instance: instance,
	}

	response, err := s.manager.HandleCommit(request)
	c.Assert(err, gocheck.IsNil)
	c.Assert(response, gocheck.NotNil)
	c.Assert(instance.Status, gocheck.Equals, INSTANCE_COMMITTED)
}

// tests that commits are handled properly if
// the commit if for an instance the node has
// not previously seen
func (s *CommitReplicaTest) TestHandleNewSuccess(c *gocheck.C) {
	leaderID := node.NewNodeId()
	leaderInstance := makeInstance(leaderID, []InstanceID{})
	leaderInstance.Dependencies = s.manager.getInstanceDeps(leaderInstance)
	leaderInstance.Sequence += 5

	request := &CommitRequest{
		Instance: leaderInstance,
	}

	c.Assert(s.manager.instances.ContainsID(leaderInstance.InstanceID), gocheck.Equals, false)

	response, err := s.manager.HandleCommit(request)
	c.Assert(err, gocheck.IsNil)
	c.Assert(response, gocheck.NotNil)

	instance := s.manager.instances.Get(leaderInstance.InstanceID)

	c.Assert(instance.Status, gocheck.Equals, INSTANCE_COMMITTED)
}

// when a commit message is received, the instance should
// be asynchronously executed against the store
func (s *CommitReplicaTest) TestHandleCommitAsyncExecute(c *gocheck.C) {
	c.Skip("no longer executing on commit")
	oldExecute := managerExecuteInstance
	defer func() { managerExecuteInstance = oldExecute }()

	executeCalled := false
	managerExecuteInstance = func(m *Manager, i *Instance) (store.Value, error) {
		executeCalled = true
		return nil, nil
	}

	instance := s.manager.makeInstance(getBasicInstruction())
	err := s.manager.acceptInstance(instance, false)
	c.Assert(err, gocheck.IsNil)

	// sanity check
	c.Assert(instance.Status, gocheck.Equals, INSTANCE_ACCEPTED)

	request := &CommitRequest{
		Instance: instance,
	}

	response, err := s.manager.HandleCommit(request)
	c.Assert(err, gocheck.IsNil)
	c.Assert(response, gocheck.NotNil)
	c.Assert(instance.Status, gocheck.Equals, INSTANCE_COMMITTED)

	// yield to the execution thread
	runtime.Gosched()
	c.Assert(executeCalled, gocheck.Equals, true)
}
