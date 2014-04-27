package consensus

import (
	"sync"
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
	s.manager.acceptInstance(replicaInstance, false)
	originalBallot := replicaInstance.MaxBallot

	leaderInstance, _ := replicaInstance.Copy()
	leaderInstance.Dependencies = append(leaderInstance.Dependencies, NewInstanceID())

	c.Assert(len(replicaInstance.Dependencies), gocheck.Equals, 4)
	c.Check(replicaInstance.executeTimeout.IsZero(), gocheck.Equals, true)

	oldStatCommitCount := s.manager.stats.(*mockStatter).counters["commit.instance.count"]
	err := s.manager.commitInstance(leaderInstance, false)
	c.Assert(err, gocheck.IsNil)

	c.Check(replicaInstance.Status, gocheck.Equals, INSTANCE_COMMITTED)
	c.Check(replicaInstance.MaxBallot, gocheck.Equals, originalBallot)
	c.Check(len(replicaInstance.Dependencies), gocheck.Equals, 5)
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
	leaderInstance := makeInstance(node.NewNodeId(), makeDependencies(4))

	// sanity checks
	c.Assert(s.manager.instances.Contains(leaderInstance), gocheck.Equals, false)


	err := s.manager.commitInstance(leaderInstance, false)
	c.Assert(err, gocheck.IsNil)

	c.Assert(s.manager.instances.Contains(leaderInstance), gocheck.Equals, true)

	replicaInstance := s.manager.instances.Get(leaderInstance.InstanceID)

	c.Check(replicaInstance.Status, gocheck.Equals,  INSTANCE_COMMITTED)
	c.Check(leaderInstance.Status, gocheck.Equals, INSTANCE_COMMITTED)
	c.Check(len(replicaInstance.Dependencies), gocheck.Equals, 4)
}

// tests that an instance is not marked as committed,
// or added to the committed set if it already has
// been executed
func (s *CommitInstanceTest) TestCommitExecutedFailure(c *gocheck.C) {
	replicaInstance := makeInstance(s.manager.GetLocalID(), makeDependencies(4))
	replicaInstance.Status = INSTANCE_EXECUTED

	s.manager.instances.Add(replicaInstance)
	s.manager.executed = append(s.manager.executed, replicaInstance.InstanceID)

	leaderInstance, _ := replicaInstance.Copy()
	leaderInstance.Status = INSTANCE_ACCEPTED

	// sanity checks
	c.Assert(s.manager.executed[len(s.manager.executed) - 1] == replicaInstance.InstanceID, gocheck.Equals, true)

	err := s.manager.commitInstance(leaderInstance, false)
	c.Assert(err, gocheck.FitsTypeOf, InvalidStatusUpdateError{})

	// check set memberships haven't changed
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
}

// tests that instances with a commitNotify Cond object
// calls broadcast when the instance is committed
func (s *CommitInstanceTest) TestCommitInstanceBroadcast(c *gocheck.C) {

}

func (s *CommitInstanceTest) TestCommitNoop(c *gocheck.C) {

}

// tests that instance dependencies are marked as acknowledged on commit
func (s *CommitInstanceTest) TestReportAcknowledged(c *gocheck.C) {
	var err error
	instance := s.manager.makeInstance(getBasicInstruction())
	toAcknowledge := NewInstanceID()
	instance.Dependencies = []InstanceID{toAcknowledge}

	// check that this instance hasn't already been somehow acknowledged
	depsNode := s.manager.depsMngr.deps.get("a")
	c.Assert(depsNode.acknowledged.Contains(instance.InstanceID), gocheck.Equals, false)

	err = s.manager.commitInstance(instance, false)
	c.Assert(err, gocheck.IsNil)

	// check that this instance's deps has been acknowledged, but it hasn't
	c.Check(depsNode.acknowledged.Contains(toAcknowledge), gocheck.Equals, true)
	c.Check(depsNode.acknowledged.Contains(instance.InstanceID), gocheck.Equals, false)
	c.Check(depsNode.executed.Contains(instance.InstanceID), gocheck.Equals, false)
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
	// all replicas agree
	wg := &sync.WaitGroup{}
	wg.Add(len(s.replicas))
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		wg.Done()
		return &CommitResponse{}, nil
	}

	for _, replica := range s.replicas {
		replica.messageHandler = responseFunc
	}

	err := s.manager.sendCommit(s.instance, transformMockNodeArray(s.replicas))
	c.Assert(err, gocheck.IsNil)

	wg.Wait()

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

	wg := &sync.WaitGroup{}
	wg.Add(len(s.replicas))

	// all replicas agree
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		request := m.(*CommitRequest)
		sentBallot = request.Instance.MaxBallot
		wg.Done()

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
	wg.Wait()

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
	var err error
	leaderID := node.NewNodeId()
	leaderInstance := makeInstance(leaderID, []InstanceID{})
	leaderInstance.Dependencies, err = s.manager.getInstanceDeps(leaderInstance)
	c.Assert(err, gocheck.IsNil)

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
