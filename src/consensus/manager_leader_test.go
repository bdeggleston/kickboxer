/**
tests the command leaders protocol execution
*/
package consensus

import (
	"launchpad.net/gocheck"
)

type LeaderTest struct {
	baseManagerTest
}

var _ = gocheck.Suite(&LeaderTest{})

// tests that execution will fail if the command leader
// (the node running ExecuteQuery can't find
// itself in the list of given replicas
func (s *LeaderTest) TestNonReplicaLeaderFailure(c *gocheck.C) {

}

// tests that the comand leader aborts if there's an
// error creating a new instance
func (s *LeaderTest) TestInstanceCreationPersistenceError(c *gocheck.C) {

}
