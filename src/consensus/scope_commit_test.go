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
	baseScopeTest
}

var _ = gocheck.Suite(&CommitInstanceTest{})

// tests that an instance is marked as committed,
// added to the committed set, removed from the
// inProgress set, and persisted if it hasn't
// already been executed
func (s *CommitInstanceTest) TestExistingSuccessCase(c *gocheck.C) {
	replicaInstance := makeInstance(s.manager.GetLocalID(), makeDependencies(4))
	s.scope.maxSeq = 3
	replicaInstance.Sequence = s.scope.maxSeq
	s.scope.acceptInstance(replicaInstance, false)
	originalBallot := replicaInstance.MaxBallot

	leaderInstance := copyInstance(replicaInstance)
	leaderInstance.Sequence++
	leaderInstance.Dependencies = append(leaderInstance.Dependencies, NewInstanceID())

	// sanity checks
	c.Assert(s.scope.inProgress.Contains(leaderInstance), gocheck.Equals, true)
	c.Assert(s.scope.committed.Contains(leaderInstance), gocheck.Equals, false)

	c.Assert(len(replicaInstance.Dependencies), gocheck.Equals, 4)
	c.Assert(replicaInstance.Sequence, gocheck.Equals, uint64(3))
	c.Assert(s.scope.maxSeq, gocheck.Equals, uint64(3))
	c.Check(replicaInstance.executeTimeout.IsZero(), gocheck.Equals, true)

	oldStatCommitCount := s.scope.manager.stats.(*mockStatter).counters["commit.instance.count"]
	err := s.scope.commitInstance(leaderInstance, false)
	c.Assert(err, gocheck.IsNil)

	// test bookkeeping
	c.Assert(s.scope.inProgress.Contains(leaderInstance), gocheck.Equals, false)
	c.Assert(s.scope.committed.Contains(leaderInstance), gocheck.Equals, true)

	c.Check(replicaInstance.Status, gocheck.Equals, INSTANCE_COMMITTED)
	c.Check(replicaInstance.MaxBallot, gocheck.Equals, originalBallot)
	c.Check(len(replicaInstance.Dependencies), gocheck.Equals, 5)
	c.Check(replicaInstance.Sequence, gocheck.Equals, uint64(4))
	c.Check(s.scope.maxSeq, gocheck.Equals, uint64(4))
	c.Check(s.scope.manager.stats.(*mockStatter).counters["commit.instance.count"], gocheck.Equals, oldStatCommitCount + 1)
	c.Check(replicaInstance.executeTimeout.IsZero(), gocheck.Equals, false)
	c.Check(replicaInstance.executeTimeout.After(time.Now()), gocheck.Equals, true)
}

func (s *CommitInstanceTest) TestBallotIncrement(c *gocheck.C) {
	instance := makeInstance(s.manager.GetLocalID(), makeDependencies(4))
	originalBallot := instance.MaxBallot
	err := s.scope.commitInstance(instance, true)
	c.Assert(err, gocheck.IsNil)

	c.Check(instance.MaxBallot, gocheck.Equals, originalBallot + 1)
}
// tests that an instance is marked as committed,
// added to the instances and committed set, and
// persisted if the instance hasn't been seen before
func (s *CommitInstanceTest) TestNewSuccessCase(c *gocheck.C) {
	s.scope.maxSeq = 3

	leaderInstance := makeInstance(node.NewNodeId(), makeDependencies(4))
	leaderInstance.Sequence = s.scope.maxSeq + 2

	// sanity checks
	c.Assert(s.scope.instances.Contains(leaderInstance), gocheck.Equals, false)
	c.Assert(s.scope.inProgress.Contains(leaderInstance), gocheck.Equals, false)
	c.Assert(s.scope.committed.Contains(leaderInstance), gocheck.Equals, false)


	err := s.scope.commitInstance(leaderInstance, false)
	c.Assert(err, gocheck.IsNil)

	c.Assert(s.scope.instances.Contains(leaderInstance), gocheck.Equals, true)
	c.Assert(s.scope.inProgress.Contains(leaderInstance), gocheck.Equals, false)
	c.Assert(s.scope.committed.Contains(leaderInstance), gocheck.Equals, true)

	replicaInstance := s.scope.instances.Get(leaderInstance.InstanceID)

	c.Check(replicaInstance.Status, gocheck.Equals,  INSTANCE_COMMITTED)
	c.Check(leaderInstance.Status, gocheck.Equals, INSTANCE_COMMITTED)
	c.Check(len(replicaInstance.Dependencies), gocheck.Equals, 4)
	c.Check(replicaInstance.Sequence, gocheck.Equals, uint64(5))
	c.Check(s.scope.maxSeq, gocheck.Equals, uint64(5))
}

// tests that an instance is not marked as committed,
// or added to the committed set if it already has
// been executed
func (s *CommitInstanceTest) TestCommitExecutedFailure(c *gocheck.C) {
	replicaInstance := makeInstance(s.manager.GetLocalID(), makeDependencies(4))
	s.scope.maxSeq = 3
	replicaInstance.Sequence = s.scope.maxSeq
	replicaInstance.Status = INSTANCE_EXECUTED

	s.scope.instances.Add(replicaInstance)
	s.scope.executed = append(s.scope.executed, replicaInstance.InstanceID)

	leaderInstance := copyInstance(replicaInstance)
	leaderInstance.Status = INSTANCE_ACCEPTED

	// sanity checks
	c.Assert(s.scope.committed.Contains(leaderInstance), gocheck.Equals, false)
	c.Assert(s.scope.inProgress.Contains(leaderInstance), gocheck.Equals, false)
	c.Assert(s.scope.executed[len(s.scope.executed) - 1] == replicaInstance.InstanceID, gocheck.Equals, true)

	err := s.scope.commitInstance(leaderInstance, false)
	c.Assert(err, gocheck.FitsTypeOf, InvalidStatusUpdateError{})

	// check set memberships haven't changed
	c.Assert(s.scope.committed.Contains(leaderInstance), gocheck.Equals, false)
	c.Assert(s.scope.inProgress.Contains(leaderInstance), gocheck.Equals, false)
	c.Assert(s.scope.executed[len(s.scope.executed) - 1] == replicaInstance.InstanceID, gocheck.Equals, true)
	c.Check(replicaInstance.Status, gocheck.Equals, INSTANCE_EXECUTED)
}

// if an instance is being committed twice
// which is possible if there's an explicit
// prepare, it should copy some attributes,
// (noop), and not overwrite any existing
// instances references in the scope's containers
func (s *CommitInstanceTest) TestRepeatCommit(c *gocheck.C ) {
	var err error
	instance := s.scope.makeInstance(getBasicInstruction())
	repeat := copyInstance(instance)

	err = s.scope.commitInstance(instance, false)
	c.Assert(err, gocheck.IsNil)

	err = s.scope.commitInstance(repeat, false)
	c.Assert(err, gocheck.IsNil)
	c.Assert(s.scope.instances.Get(instance.InstanceID), gocheck.Equals, instance)
	c.Assert(s.scope.instances.Get(instance.InstanceID), gocheck.Not(gocheck.Equals), repeat)
	c.Assert(s.scope.committed.Get(instance.InstanceID), gocheck.Equals, instance)
	c.Assert(s.scope.committed.Get(instance.InstanceID), gocheck.Not(gocheck.Equals), repeat)
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
	s.instance = s.scope.makeInstance(getBasicInstruction())
	var err error

	err = s.scope.preAcceptInstance(s.instance, false)
	c.Assert(err, gocheck.IsNil)
	err = s.scope.acceptInstance(s.instance, false)
	c.Assert(err, gocheck.IsNil)
	err = s.scope.commitInstance(s.instance, false)
	c.Assert(err, gocheck.IsNil)
}

// tests that calling sendCommit sends commit requests
// to the other replicas
func (s *CommitLeaderTest) TestSendSuccess(c *gocheck.C) {
	err := s.scope.sendCommit(s.instance, transformMockNodeArray(s.replicas))
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


type CommitReplicaTest struct {
	baseScopeTest
}

var _ = gocheck.Suite(&CommitReplicaTest{})

// tests that an instance is marked as committed when
// a commit request is recived
func (s *CommitReplicaTest) TestHandleSuccess(c *gocheck.C) {
	instance := s.scope.makeInstance(getBasicInstruction())
	err := s.scope.acceptInstance(instance, false)
	c.Assert(err, gocheck.IsNil)

	// sanity check
	c.Assert(instance.Status, gocheck.Equals, INSTANCE_ACCEPTED)

	request := &CommitRequest{
		Scope: s.scope.name,
		Instance: instance,
	}

	response, err := s.scope.HandleCommit(request)
	c.Assert(err, gocheck.IsNil)
	c.Assert(response, gocheck.NotNil)
	c.Assert(instance.Status, gocheck.Equals, INSTANCE_COMMITTED)
}

// tests that commits are handled properly if
// the commit if for an instance the node has
// not previously seen
func (s *CommitReplicaTest) TestHandleNewSuccess(c *gocheck.C) {
	leaderID := node.NewNodeId()
	leaderInstance := makeInstance(leaderID, s.scope.getCurrentDepsUnsafe())
	leaderInstance.Sequence += 5

	request := &CommitRequest{
		Scope: s.scope.name,
		Instance: leaderInstance,
	}

	c.Assert(s.scope.instances.ContainsID(leaderInstance.InstanceID), gocheck.Equals, false)

	response, err := s.scope.HandleCommit(request)
	c.Assert(err, gocheck.IsNil)
	c.Assert(response, gocheck.NotNil)

	instance := s.scope.instances.Get(leaderInstance.InstanceID)

	c.Assert(instance.Status, gocheck.Equals, INSTANCE_COMMITTED)
}

// when a commit message is received, the instance should
// be asynchronously executed against the store
func (s *CommitReplicaTest) TestHandleCommitAsyncExecute(c *gocheck.C) {
	oldExecute := scopeExecuteInstance
	defer func() { scopeExecuteInstance = oldExecute }()

	executeCalled := false
	scopeExecuteInstance = func(s *Scope, i *Instance) (store.Value, error) {
		executeCalled = true
		return nil, nil
	}

	instance := s.scope.makeInstance(getBasicInstruction())
	err := s.scope.acceptInstance(instance, false)
	c.Assert(err, gocheck.IsNil)

	// sanity check
	c.Assert(instance.Status, gocheck.Equals, INSTANCE_ACCEPTED)

	request := &CommitRequest{
		Scope: s.scope.name,
		Instance: instance,
	}

	response, err := s.scope.HandleCommit(request)
	c.Assert(err, gocheck.IsNil)
	c.Assert(response, gocheck.NotNil)
	c.Assert(instance.Status, gocheck.Equals, INSTANCE_COMMITTED)

	// yield to the execution thread
	runtime.Gosched()
	c.Assert(executeCalled, gocheck.Equals, true)
}
