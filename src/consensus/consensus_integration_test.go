package consensus

import (
	"launchpad.net/gocheck"
)

/*
   long running integration tests. Each integration iteration should run on one core, with
	inter-node communication running through a message broker, which will randomly add failure
	scenarios. At the beginning of each iteration, the random number generator should be seeded
	with a value that can be recorded, so failures can be later played back and debugged. The
	mockNode keeps a log of all instructions it has been given. Message logs between nodes
	should be regularly compared. If any message lists are not equal, or one is not a subset
	of the other, an error has occurred, and needs to be debugged.
		Failure Scenarios:
			* Network partition
			* Unresponsive node
			* Deloyed / Out of order messages
			* Missing messages
		Also to do:
			* wrap timeout event creation in a method that can be mocked out so the
				test runner can send out random, repeatable timeout events
 */

type ConsensusIntegrationTest struct {
	baseScopeTest
}

var _ = gocheck.Suite(&ConsensusIntegrationTest{})
