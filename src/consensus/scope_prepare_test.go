package consensus

import (
	"fmt"
	"runtime"
	"time"
)


import (
	"launchpad.net/gocheck"
)

type basePrepareTest struct {
	baseScopeTest

	oldScopePreparePhase func(*Scope, *Instance) error
	oldScopePreparePhase1 func(*Scope, *Instance) (*Instance, error)
	oldScopePreparePhase2 func(*Scope, *Instance, *Instance) error

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
type PreparePhaseSendTest struct {
	baseScopeTest
}

var _ = gocheck.Suite(&PreparePhaseSendTest{})

// tests the receive prepare response method
type PreparePhaseReceiveTest struct {
	basePrepareTest
}

var _ = gocheck.Suite(&PreparePhaseSendTest{})

// tests the analyzePrepareResponses method
type AnalyzePrepareResponsesTest struct {

}

var _ = gocheck.Suite(&PreparePhaseTest{})

// tests the prepare phase method
type PreparePhaseTest struct {
	basePrepareTest
}

var _ = gocheck.Suite(&PreparePhaseTest{})

// tests that the preparePhase doesn't actually do anything
// if the instance has been committed
func (s *PreparePhaseTest) TestInstanceCommittedAbort(c *gocheck.C) {
	var err error
	err = s.scope.commitInstance(s.instance)
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

// tests the prepare phase method
type PreparePhase2Test struct {
	basePrepareTest
}

var _ = gocheck.Suite(&PreparePhase2Test{})

// tests that receiving prepare responses, where the highest
// balloted instance's status was preaccepted, a preaccept phase
// is initiated, and the accept phase is skipped if there are no
// dependency mismatches
func (s *PreparePhase2Test) TestPreAcceptedSuccess(c *gocheck.C) {
	remoteInstance := copyInstance(s.instance)
	remoteInstance.Status = INSTANCE_PREACCEPTED

	// patch methods
	s.patchPreAccept(false, nil)
	s.patchAccept(nil)
	s.patchCommit(nil)

	err := scopePreparePhase2(s.scope, s.instance, remoteInstance)
	c.Assert(err, gocheck.IsNil)

	c.Check(s.preAcceptCalls, gocheck.Equals, 1)
	c.Check(s.acceptCalls, gocheck.Equals, 0)
	c.Check(s.commitCalls, gocheck.Equals, 1)
}

// tests that receiving prepare responses, where the highest
// balloted instance's status was preaccepted, a preaccept phase
// is initiated, and the accept phase is skipped if there are
// dependency mismatches
func (s *PreparePhase2Test) TestPreAcceptedChangeSuccess(c *gocheck.C) {
	remoteInstance := copyInstance(s.instance)
	remoteInstance.Status = INSTANCE_PREACCEPTED

	// patch methods
	s.patchPreAccept(false, nil)
	s.patchAccept(nil)
	s.patchCommit(nil)

	err := scopePreparePhase2(s.scope, s.instance, remoteInstance)
	c.Assert(err, gocheck.IsNil)

	c.Check(s.preAcceptCalls, gocheck.Equals, 1)
	c.Check(s.acceptCalls, gocheck.Equals, 0)
	c.Check(s.commitCalls, gocheck.Equals, 1)
}

// tests that receiving prepare responses, where the highest
// balloted instance's status was preaccepted, a preaccept phase
// is initiated, and the method returns if pre accept returns an
// error
func (s *PreparePhase2Test) TestPreAcceptedFailure(c *gocheck.C) {
	remoteInstance := copyInstance(s.instance)
	remoteInstance.Status = INSTANCE_PREACCEPTED

	// patch methods
	s.patchPreAccept(false, fmt.Errorf("Nope"))

	err := scopePreparePhase2(s.scope, s.instance, remoteInstance)
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

	// patch methods
	s.patchAccept(nil)
	s.patchCommit(nil)

	err := scopePreparePhase2(s.scope, s.instance, remoteInstance)
	c.Assert(err, gocheck.IsNil)

	c.Check(s.preAcceptCalls, gocheck.Equals, 0)
	c.Check(s.acceptCalls, gocheck.Equals, 1)
	c.Check(s.commitCalls, gocheck.Equals, 1)
}

// tests that receiving prepare responses, where the highest
// balloted instance's status was accepted, an accept phase
// is initiated, and the method returns if accept returns an error
func (s *PreparePhase2Test) TestAcceptFailure(c *gocheck.C) {
	remoteInstance := copyInstance(s.instance)
	remoteInstance.Status = INSTANCE_ACCEPTED

	// patch methods
	s.patchAccept(fmt.Errorf("Nope"))

	err := scopePreparePhase2(s.scope, s.instance, remoteInstance)
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

	// patch methods
	s.patchCommit(nil)

	err := scopePreparePhase2(s.scope, s.instance, remoteInstance)
	c.Assert(err, gocheck.IsNil)

	c.Check(s.preAcceptCalls, gocheck.Equals, 0)
	c.Check(s.acceptCalls, gocheck.Equals, 0)
	c.Check(s.commitCalls, gocheck.Equals, 1)
}

// tests that receiving prepare responses, where the highest
// balloted instance's status was committed, a commit phase
// is initiated, and the method returns if commit returns an error
func (s *PreparePhase2Test) TestCommitFailure(c *gocheck.C) {
	remoteInstance := copyInstance(s.instance)
	remoteInstance.Status = INSTANCE_COMMITTED

	// patch methods
	s.patchCommit(fmt.Errorf("Nope"))

	err := scopePreparePhase2(s.scope, s.instance, remoteInstance)
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

	// patch methods
	s.patchCommit(nil)

	err := scopePreparePhase2(s.scope, s.instance, remoteInstance)
	c.Assert(err, gocheck.IsNil)

	c.Check(s.preAcceptCalls, gocheck.Equals, 0)
	c.Check(s.acceptCalls, gocheck.Equals, 0)
	c.Check(s.commitCalls, gocheck.Equals, 1)
}

// tests that receiving prepare responses, where the highest
// balloted instance's status was executed, a commit phase
// is initiated, and the method returns if commit returns an error
func (s *PreparePhase2Test) TestExecutedFailure(c *gocheck.C) {
	remoteInstance := copyInstance(s.instance)
	remoteInstance.Status = INSTANCE_EXECUTED

	// patch methods
	s.patchCommit(fmt.Errorf("Nope"))

	err := scopePreparePhase2(s.scope, s.instance, remoteInstance)
	c.Assert(err, gocheck.NotNil)

	c.Check(s.preAcceptCalls, gocheck.Equals, 0)
	c.Check(s.acceptCalls, gocheck.Equals, 0)
	c.Check(s.commitCalls, gocheck.Equals, 1)
}

// tests that the instance's ballot is updated if prepare
// responses return higher ballot numbers
func (s *PreparePhase2Test) TestBallotUpdate(c *gocheck.C) {

}

// tests that a noop is committed if no other nodes are aware
// of the instance in the prepare phase
func (s *PreparePhase2Test) TestUnknownInstance(c *gocheck.C) {

}

