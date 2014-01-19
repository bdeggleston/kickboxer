package consensus

import (
	"launchpad.net/gocheck"
)

// tests the send prepare method
type PreparePhaseSendTest struct {
	baseScopeTest
}

var _ = gocheck.Suite(&PreparePhaseSendTest{})

// tests the receive prepare response method
type PreparePhaseReceiveTest struct {
	baseScopeTest
}

var _ = gocheck.Suite(&PreparePhaseSendTest{})

// tests the analyzePrepareResponses method
type AnalyzePrepareResponsesTest struct {

}

var _ = gocheck.Suite(&PreparePhaseTest{})

// tests the prepare phase method
type PreparePhaseTest struct {
	baseScopeTest
}

var _ = gocheck.Suite(&PreparePhaseTest{})

// tests that the instance's ballot is updated if prepare
// responses return higher ballot numbers
func (s *PreparePhaseTest) TestBallotUpdate(c *gocheck.C) {

}

// tests that a noop is committed if less than a quorum of
// instance is aware of the instance in the prepare phase
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
