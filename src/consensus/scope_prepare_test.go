package consensus

import (
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
}

func (s *basePrepareTest) SetUpTest(c *gocheck.C) {
	s.baseScopeTest.SetUpTest(c)
	s.oldScopePreparePhase = scopePreparePhase
	s.oldScopePreparePhase1 = scopePreparePhase1
	s.oldScopePreparePhase2 = scopePreparePhase2

	s.instance = s.scope.makeInstance(getBasicInstruction())
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

// tests that the instance's ballot is updated if prepare
// responses return higher ballot numbers
func (s *PreparePhaseTest) TestBallotUpdate(c *gocheck.C) {

}

// tests that a noop is committed if no other nodes are aware
// of the instance in the prepare phase
func (s *PreparePhaseTest) TestUnknownInstance(c *gocheck.C) {

}

// tests that receiving prepare responses, where the highest
// balloted instance's status was preaccepted, a preaccept phase
// is initiated, and the accept phase is skipped if there are no
// dependency mismatches
func (s *PreparePhaseTest) TestPreAcceptedSuccess(c *gocheck.C) {

}

// tests that receiving prepare responses, where the highest
// balloted instance's status was preaccepted, a preaccept phase
// is initiated, and the accept phase is skipped if there are
// dependency mismatches
func (s *PreparePhaseTest) TestPreAcceptedChangeSuccess(c *gocheck.C) {

}

// tests that receiving prepare responses, where the highest
// balloted instance's status was preaccepted, a preaccept phase
// is initiated, and the method returns if pre accept returns an
// error
func (s *PreparePhaseTest) TestPreAcceptedFailure(c *gocheck.C) {

}
