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
	"message"
)

type basePrepareTest struct {
	baseScopeTest

	oldScopePreparePhase func(*Scope, *Instance) error
	oldScopePreparePhase1 func(*Scope, *Instance) (*Instance, error)
	oldScopePreparePhase2 func(*Scope, *Instance, []*PrepareResponse) error

	instance *Instance

	preAcceptCalls int
	acceptCalls int
	commitCalls int
}

func (s *basePrepareTest) SetUpTest(c *gocheck.C) {
	s.baseScopeTest.SetUpTest(c)
	s.oldScopePreparePhase = scopePreparePhase
	s.oldScopePreparePhase1 = scopePreparePhase1
	s.oldScopePreparePhase2 = scopePreparePhase2

	s.instance = s.scope.makeInstance(getBasicInstruction())

	s.preAcceptCalls = 0
	s.acceptCalls = 0
	s.commitCalls = 0
}

func (s *basePrepareTest) patchPreAccept(r bool, e error) {
	scopePreAcceptPhase = func(_ *Scope, _ *Instance) (bool, error) {
		s.preAcceptCalls++
		return r, e
	}
}

func (s *basePrepareTest) patchAccept(e error) {
	scopeAcceptPhase = func(_ *Scope, _ *Instance) (error) {
		s.acceptCalls++
		return e
	}
}

func (s *basePrepareTest) patchCommit(e error) {
	scopeCommitPhase = func(_ *Scope, _ *Instance) (error) {
		s.commitCalls++
		return e
	}
}


func (s *basePrepareTest) TearDownTest(c *gocheck.C) {
	scopePreparePhase = s.oldScopePreparePhase
	scopePreparePhase1 = s.oldScopePreparePhase1
	scopePreparePhase2 = s.oldScopePreparePhase2
}

// tests the send prepare method
type PrepareLeaderTest struct {
	baseReplicaTest

	instance *Instance
	oldPrepareTimeout uint64
}

var _ = gocheck.Suite(&PrepareLeaderTest{})

func (s *PrepareLeaderTest) SetUpSuite(c *gocheck.C) {
	s.baseReplicaTest.SetUpSuite(c)
	s.oldPrepareTimeout = PREPARE_TIMEOUT
	PREPARE_TIMEOUT = 50
}

func (s *PrepareLeaderTest) TearDownSuite(c *gocheck.C) {
	PREPARE_TIMEOUT = s.oldPrepareTimeout
}

func (s *PrepareLeaderTest) SetUpTest(c *gocheck.C) {
	s.baseReplicaTest.SetUpTest(c)
	s.instance = s.scope.makeInstance(getBasicInstruction())
}

// tests message
func (s *PrepareLeaderTest) TestSendSuccess(c *gocheck.C) {
	// all replicas agree
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		return &PrepareResponse{
			Accepted:         true,
			Instance:        s.instance,
		}, nil
	}

	for _, replica := range s.replicas {
		replica.messageHandler = responseFunc
	}

	responses, err := scopeSendPrepare(s.scope, s.instance)
	runtime.Gosched()
	c.Assert(err, gocheck.IsNil)

	c.Check(len(responses) >= 3, gocheck.Equals, true)
}

func (s *PrepareLeaderTest) TestQuorumFailure(c *gocheck.C) {
	// all replicas agree
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		return &PrepareResponse{
			Accepted:         true,
			Instance:        s.instance,
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

	responses, err := scopeSendPrepare(s.scope, s.instance)
	runtime.Gosched()
	c.Assert(responses, gocheck.IsNil)
	c.Assert(err, gocheck.NotNil)
	c.Check(err, gocheck.FitsTypeOf, TimeoutError{})
}

// tests that the local instance's ballot is updated from
// rejected prepare responses
func (s *PrepareLeaderTest) TestBallotFailure(c *gocheck.C) {
	s.instance.MaxBallot = 5
	// all replicas agree
	responseFunc := func(n *mockNode, m message.Message) (message.Message, error) {
		inst := copyInstance(s.instance)
		inst.MaxBallot++
		return &PrepareResponse{
			Accepted:         false,
			Instance:        inst,
		}, nil
	}

	for _, replica := range s.replicas {
		replica.messageHandler = responseFunc
	}

	_, err := scopeSendPrepare(s.scope, s.instance)
	runtime.Gosched()
	c.Assert(err, gocheck.IsNil)

	c.Check(s.instance.MaxBallot, gocheck.Equals, uint32(6))
}

// tests the analyzePrepareResponses method
type AnalyzePrepareResponsesTest struct {
	basePrepareTest
}

var _ = gocheck.Suite(&PreparePhaseTest{})


// tests that the instance with the highest ballot/status is returned
func (s *AnalyzePrepareResponsesTest) TestSuccessCase(c *gocheck.C) {
	responses := make([]*PrepareResponse, 0)
	addResponse := func(ballot uint32, status InstanceStatus) {
		instance := copyInstance(s.instance)
		instance.MaxBallot = ballot
		instance.Status = status
		response := &PrepareResponse{
			Accepted: true,
			Instance: instance,
		}
		responses = append(responses, response)
	}
	addResponse(uint32(4), INSTANCE_PREACCEPTED)
	addResponse(uint32(4), INSTANCE_ACCEPTED)
	addResponse(uint32(4), INSTANCE_EXECUTED)
	addResponse(uint32(5), INSTANCE_PREACCEPTED)
	addResponse(uint32(5), INSTANCE_ACCEPTED)
	addResponse(uint32(5), INSTANCE_COMMITTED)

	instance := s.scope.analyzePrepareResponses(responses)
	c.Assert(instance, gocheck.NotNil)
	c.Check(instance.MaxBallot, gocheck.Equals, uint32(5))
	c.Check(instance.Status, gocheck.Equals, INSTANCE_COMMITTED)
}

// tests that a mix of responses with nil instances, and not nil instances
// is safe from nil pointer errors
func (s *AnalyzePrepareResponsesTest) TestMixedNilResponses(c *gocheck.C) {

}

// tests the prepare phase method
type PreparePhaseTest struct {
	basePrepareTest
}

var _ = gocheck.Suite(&PreparePhaseTest{})

// tests that the preparePhase doesn't actually do anything
// if the instance has been committed
func (s *PreparePhaseTest) TestInstanceCommittedAbort(c *gocheck.C) {
	var err error
	err = s.scope.commitInstance(s.instance, false)
	c.Assert(err, gocheck.IsNil)

	prepareCalls := 0
	scopePreparePhase = func(s *Scope, i *Instance) error {
		prepareCalls++
		return nil
	}
	// sanity check
	c.Assert(prepareCalls, gocheck.Equals, 0)

	err = s.scope.preparePhase(s.instance)
	c.Assert(err, gocheck.IsNil)
	c.Check(prepareCalls, gocheck.Equals, 0)
}

// tests that the prepare phase immediately starts if the
// given instance is past it's commit grace period
func (s *PreparePhaseTest) TestCommitExpiredTimeout(c *gocheck.C) {
	c.Skip("deadlock")
	s.instance.commitTimeout = time.Now().Add(time.Duration(-1) * time.Millisecond)

	prepareCalls := 0
	scopePreparePhase = func(s *Scope, i *Instance) error {
		prepareCalls++
		return nil
	}
	// sanity check
	c.Assert(prepareCalls, gocheck.Equals, 0)

	err := s.scope.preparePhase(s.instance)
	c.Assert(err, gocheck.IsNil)
	c.Check(prepareCalls, gocheck.Equals, 1)
	c.Check(s.scope.statCommitTimeout, gocheck.Equals, uint64(1))
	c.Check(s.scope.statCommitTimeoutWait, gocheck.Equals, uint64(0))
}

// tests that the prepare phase starts after the commit grace
// period if another goroutine does not commit it first
func (s *PreparePhaseTest) TestCommitTimeout(c *gocheck.C) {
	c.Skip("deadlock")
	s.instance.commitTimeout = time.Now().Add(time.Duration(2) * time.Millisecond)

	prepareCalls := 0
	scopePreparePhase = func(s *Scope, i *Instance) error {
		prepareCalls++
		return nil
	}
	// sanity check
	c.Assert(prepareCalls, gocheck.Equals, 0)

	err := s.scope.preparePhase(s.instance)
	c.Assert(err, gocheck.IsNil)
	c.Check(prepareCalls, gocheck.Equals, 1)
	c.Check(s.scope.statCommitTimeout, gocheck.Equals, uint64(1))
	c.Check(s.scope.statCommitTimeoutWait, gocheck.Equals, uint64(1))
}

// tests that the prepare phase will wait on the commit notify
// cond if it's within the commit grace period, and will abort
// the prepare if another goroutine commits the instance first
func (s *PreparePhaseTest) TestCommitNotify(c *gocheck.C) {
	c.Skip("deadlock")
	s.instance.commitTimeout = time.Now().Add(time.Duration(10) * time.Second)

	depNotify := makeConditional()
	s.scope.commitNotify[s.instance.InstanceID] = depNotify

	prepareCalls := 0
	scopePreparePhase = func(s *Scope, i *Instance) error {
		prepareCalls++
		return nil
	}
	// sanity check
	c.Assert(prepareCalls, gocheck.Equals, 0)

	var err error
	go func() { err = s.scope.preparePhase(s.instance) }()
	runtime.Gosched()

	// release wait
	depNotify.Broadcast()
	runtime.Gosched()

	c.Assert(err, gocheck.IsNil)
	c.Check(prepareCalls, gocheck.Equals, 0)
	c.Check(s.scope.statCommitTimeout, gocheck.Equals, uint64(0))
	c.Check(s.scope.statCommitTimeoutWait, gocheck.Equals, uint64(1))
}

// tests that the prepare mutex prevents multiple goroutines from
// trying to run a prepare phase on the same instance simultaneously
func (s *PreparePhaseTest) TestPrepareMutex(c *gocheck.C) {
	// TODO: this
}

// tests the prepare phase method
type PrepareCheckResponsesTest struct {
	basePrepareTest
}

var _ = gocheck.Suite(&PrepareCheckResponsesTest{})

// if all of the responses have been accepted, no error should
// be returned
func (s *PrepareCheckResponsesTest) TestSuccessCase(c *gocheck.C) {

}

// if any of the responses have been rejected, an error should be returned
func (s *PrepareCheckResponsesTest) TestRejectedMessageFailure(c *gocheck.C) {

}

// if any of the respones have been rejected, and the remote instance
// from the rejecting node is accepted, committed, or executed, the
// local instance should be update with it's status and attributes
func (s *PrepareCheckResponsesTest) TestHigherStatusUpdate(c *gocheck.C) {

}

// tests the prepare phase method
type PreparePhase2Test struct {
	basePrepareTest
}

var _ = gocheck.Suite(&PreparePhase2Test{})

// tests that receiving prepare responses, where the highest
// balloted instance's status was preaccepted, a preaccept phase
// is initiated
func (s *PreparePhase2Test) TestPreAcceptedSuccess(c *gocheck.C) {
	remoteInstance := copyInstance(s.instance)
	remoteInstance.Status = INSTANCE_PREACCEPTED
	responses := []*PrepareResponse{
		&PrepareResponse{Accepted: true, Instance: remoteInstance},
	}

	// patch methods
	s.patchPreAccept(true, nil)
	s.patchAccept(nil)
	s.patchCommit(nil)

	err := scopePreparePhase2(s.scope, s.instance, responses)
	c.Assert(err, gocheck.IsNil)

	c.Check(s.preAcceptCalls, gocheck.Equals, 1)
	c.Check(s.acceptCalls, gocheck.Equals, 1)
	c.Check(s.commitCalls, gocheck.Equals, 1)

	// TODO: check that the remote instance is used on first phase, but local instance on all others
}

// tests that receiving prepare responses, where the highest
// balloted instance's status was preaccepted, a preaccept phase
// is initiated, and the accept phase is skipped if there are
// dependency mismatches
func (s *PreparePhase2Test) TestPreAcceptedChangeSuccess(c *gocheck.C) {
	remoteInstance := copyInstance(s.instance)
	remoteInstance.Status = INSTANCE_PREACCEPTED
	responses := []*PrepareResponse{
		&PrepareResponse{Accepted: true, Instance: remoteInstance},
	}

	// patch methods
	s.patchPreAccept(false, nil)
	s.patchAccept(nil)
	s.patchCommit(nil)

	err := scopePreparePhase2(s.scope, s.instance, responses)
	c.Assert(err, gocheck.IsNil)

	c.Check(s.preAcceptCalls, gocheck.Equals, 1)
	c.Check(s.acceptCalls, gocheck.Equals, 0)
	c.Check(s.commitCalls, gocheck.Equals, 1)
	// TODO: check that the remote instance is used on first phase, but local instance on all others
}

// tests that receiving prepare responses, where the highest
// balloted instance's status was preaccepted, a preaccept phase
// is initiated, and the method returns if pre accept returns an
// error
func (s *PreparePhase2Test) TestPreAcceptedFailure(c *gocheck.C) {
	remoteInstance := copyInstance(s.instance)
	remoteInstance.Status = INSTANCE_PREACCEPTED
	responses := []*PrepareResponse{
		&PrepareResponse{Accepted: true, Instance: remoteInstance},
	}

	// patch methods
	s.patchPreAccept(false, fmt.Errorf("Nope"))

	err := scopePreparePhase2(s.scope, s.instance, responses)
	c.Assert(err, gocheck.NotNil)

	c.Check(s.preAcceptCalls, gocheck.Equals, 1)
	c.Check(s.acceptCalls, gocheck.Equals, 0)
	c.Check(s.commitCalls, gocheck.Equals, 0)
}

// tests that receiving prepare responses, where the highest
// balloted instance's status was accepted, an accept phase
// is initiated
func (s *PreparePhase2Test) TestAcceptSuccess(c *gocheck.C) {
	remoteInstance := copyInstance(s.instance)
	remoteInstance.Status = INSTANCE_ACCEPTED
	responses := []*PrepareResponse{
		&PrepareResponse{Accepted: true, Instance: remoteInstance},
	}

	// patch methods
	s.patchAccept(nil)
	s.patchCommit(nil)

	err := scopePreparePhase2(s.scope, s.instance, responses)
	c.Assert(err, gocheck.IsNil)

	c.Check(s.preAcceptCalls, gocheck.Equals, 0)
	c.Check(s.acceptCalls, gocheck.Equals, 1)
	c.Check(s.commitCalls, gocheck.Equals, 1)

	// TODO: check that the remote instance is used on first phase, but local instance on all others
}

// tests that receiving prepare responses, where the highest
// balloted instance's status was accepted, an accept phase
// is initiated, and the method returns if accept returns an error
func (s *PreparePhase2Test) TestAcceptFailure(c *gocheck.C) {
	remoteInstance := copyInstance(s.instance)
	remoteInstance.Status = INSTANCE_ACCEPTED
	responses := []*PrepareResponse{
		&PrepareResponse{Accepted: true, Instance: remoteInstance},
	}

	// patch methods
	s.patchAccept(fmt.Errorf("Nope"))

	err := scopePreparePhase2(s.scope, s.instance, responses)
	c.Assert(err, gocheck.NotNil)

	c.Check(s.preAcceptCalls, gocheck.Equals, 0)
	c.Check(s.acceptCalls, gocheck.Equals, 1)
	c.Check(s.commitCalls, gocheck.Equals, 0)
}

// tests that receiving prepare responses, where the highest
// balloted instance's status was committed, a commit phase
// is initiated
func (s *PreparePhase2Test) TestCommitSuccess(c *gocheck.C) {
	remoteInstance := copyInstance(s.instance)
	remoteInstance.Status = INSTANCE_COMMITTED
	responses := []*PrepareResponse{
		&PrepareResponse{Accepted: true, Instance: remoteInstance},
	}

	// patch methods
	s.patchCommit(nil)

	err := scopePreparePhase2(s.scope, s.instance, responses)
	c.Assert(err, gocheck.IsNil)

	c.Check(s.preAcceptCalls, gocheck.Equals, 0)
	c.Check(s.acceptCalls, gocheck.Equals, 0)
	c.Check(s.commitCalls, gocheck.Equals, 1)

	// TODO: check that the remote instance is used on first phase, but local instance on all others
}

// tests that receiving prepare responses, where the highest
// balloted instance's status was committed, a commit phase
// is initiated, and the method returns if commit returns an error
func (s *PreparePhase2Test) TestCommitFailure(c *gocheck.C) {
	remoteInstance := copyInstance(s.instance)
	remoteInstance.Status = INSTANCE_COMMITTED
	responses := []*PrepareResponse{
		&PrepareResponse{Accepted: true, Instance: remoteInstance},
	}

	// patch methods
	s.patchCommit(fmt.Errorf("Nope"))

	err := scopePreparePhase2(s.scope, s.instance, responses)
	c.Assert(err, gocheck.NotNil)

	c.Check(s.preAcceptCalls, gocheck.Equals, 0)
	c.Check(s.acceptCalls, gocheck.Equals, 0)
	c.Check(s.commitCalls, gocheck.Equals, 1)
}

// tests that receiving prepare responses, where the highest
// balloted instance's status was executed, a commit phase
// is initiated
func (s *PreparePhase2Test) TestExecutedSuccess(c *gocheck.C) {
	remoteInstance := copyInstance(s.instance)
	remoteInstance.Status = INSTANCE_EXECUTED
	responses := []*PrepareResponse{
		&PrepareResponse{Accepted: true, Instance: remoteInstance},
	}

	// patch methods
	s.patchCommit(nil)

	err := scopePreparePhase2(s.scope, s.instance, responses)
	c.Assert(err, gocheck.IsNil)

	c.Check(s.preAcceptCalls, gocheck.Equals, 0)
	c.Check(s.acceptCalls, gocheck.Equals, 0)
	c.Check(s.commitCalls, gocheck.Equals, 1)

	// TODO: check that the remote instance is used on first phase, but local instance on all others
}

// tests that receiving prepare responses, where the highest
// balloted instance's status was executed, a commit phase
// is initiated, and the method returns if commit returns an error
func (s *PreparePhase2Test) TestExecutedFailure(c *gocheck.C) {
	remoteInstance := copyInstance(s.instance)
	remoteInstance.Status = INSTANCE_EXECUTED
	responses := []*PrepareResponse{
		&PrepareResponse{Accepted: true, Instance: remoteInstance},
	}

	// patch methods
	s.patchCommit(fmt.Errorf("Nope"))

	err := scopePreparePhase2(s.scope, s.instance, responses)
	c.Assert(err, gocheck.NotNil)

	c.Check(s.preAcceptCalls, gocheck.Equals, 0)
	c.Check(s.acceptCalls, gocheck.Equals, 0)
	c.Check(s.commitCalls, gocheck.Equals, 1)
}

// tests that the instance's ballot is updated if prepare
// responses return higher ballot numbers
func (s *PreparePhase2Test) TestBallotUpdate(c *gocheck.C) {
	s.instance.MaxBallot = 4
	remoteInstance := copyInstance(s.instance)
	remoteInstance.Status = INSTANCE_PREACCEPTED
	remoteInstance.MaxBallot = 5
	responses := []*PrepareResponse{
		&PrepareResponse{Accepted: false, Instance: remoteInstance},
	}

	// patch methods
	s.patchPreAccept(true, nil)
	s.patchAccept(nil)
	s.patchCommit(nil)

	err := scopePreparePhase2(s.scope, s.instance, responses)
	c.Assert(err, gocheck.NotNil)
	c.Assert(err, gocheck.FitsTypeOf, BallotError{})

	c.Check(s.preAcceptCalls, gocheck.Equals, 0)
	c.Check(s.acceptCalls, gocheck.Equals, 0)
	c.Check(s.commitCalls, gocheck.Equals, 0)

	c.Check(s.instance.MaxBallot, gocheck.Equals, uint32(5))
	c.Check(remoteInstance.MaxBallot, gocheck.Equals, uint32(5))
}

// tests that a noop is committed if no other nodes are aware
// of the instance in the prepare phase
func (s *PreparePhase2Test) TestUnknownInstance(c *gocheck.C) {
	responses := []*PrepareResponse{
		&PrepareResponse{Accepted: true, Instance: nil},
	}

	// patch methods
	s.patchPreAccept(true, nil)
	s.patchAccept(nil)
	s.patchCommit(nil)

	err := scopePreparePhase2(s.scope, s.instance, responses)
	c.Assert(err, gocheck.IsNil)

	c.Check(s.instance.Noop, gocheck.Equals, true)
	c.Check(s.preAcceptCalls, gocheck.Equals, 1)
	c.Check(s.acceptCalls, gocheck.Equals, 1)
	c.Check(s.commitCalls, gocheck.Equals, 1)
}

// tests the handle prepare message method
type PrepareReplicaTest struct {
	baseScopeTest

	instance *Instance
}

var _ = gocheck.Suite(&PrepareReplicaTest{})

func (s *PrepareReplicaTest) SetUpTest(c *gocheck.C) {
	s.baseScopeTest.SetUpTest(c)
	s.instance = s.scope.makeInstance(getBasicInstruction())
}

// tests that a prepare request with an incremented ballot
// number is accepted
func (s *PrepareReplicaTest) TestSuccessCase(c *gocheck.C) {
	s.scope.preAcceptInstance(s.instance, false)
	s.instance.MaxBallot = 5

	request := &PrepareRequest{
		Scope: s.scope.name,
		Ballot: s.instance.MaxBallot + 1,
		InstanceID: s.instance.InstanceID,
	}

	response, err := s.scope.HandlePrepare(request)
	c.Assert(err, gocheck.IsNil)
	c.Assert(response, gocheck.NotNil)

	c.Assert(response.Accepted, gocheck.Equals, true)
	c.Assert(response.Instance, gocheck.NotNil)
	c.Assert(response.Instance.InstanceID, gocheck.Equals, s.instance.InstanceID)
}

// tests that a prepare request with an unincremented ballot
// number is not accepted
func (s *PrepareReplicaTest) TestBallotFailure(c *gocheck.C) {
	s.scope.preAcceptInstance(s.instance, false)
	s.instance.MaxBallot = 5

	request := &PrepareRequest{
		Scope: s.scope.name,
		Ballot: s.instance.MaxBallot,
		InstanceID: s.instance.InstanceID,
	}

	response, err := s.scope.HandlePrepare(request)
	c.Assert(err, gocheck.IsNil)
	c.Assert(response, gocheck.NotNil)

	c.Assert(response.Accepted, gocheck.Equals, false)
	c.Assert(response.Instance, gocheck.NotNil)
	c.Assert(response.Instance.InstanceID, gocheck.Equals, s.instance.InstanceID)
}

// tests that a prepare request for an unknown instance is
// accepted, and a nil instance is returned
func (s *PrepareReplicaTest) TestUnknownInstance(c *gocheck.C) {
	request := &PrepareRequest{
		Scope: s.scope.name,
		Ballot: uint32(4),
		InstanceID: NewInstanceID(),
	}

	response, err := s.scope.HandlePrepare(request)
	c.Assert(err, gocheck.IsNil)
	c.Assert(response, gocheck.NotNil)

	c.Assert(response.Accepted, gocheck.Equals, true)
	c.Assert(response.Instance, gocheck.IsNil)
}

func (s *PrepareReplicaTest) TestSuccessfulPrepareMessageIncrementsBallot(c *gocheck.C) {

}
