package cluster
/**
 * Tests around nodes joining a cluster
 */

import (
	"launchpad.net/gocheck"
)

type JoinStreamTest struct {}

var _ = gocheck.Suite(&JoinStreamTest{})

// TODO: this
// tests that a node joining the cluster identifies
// the correct node to stream data from, and sends
// it a message
func (t *JoinStreamTest) TestNodeStreamsFromCorrectNode(c *gocheck.C) {

}
