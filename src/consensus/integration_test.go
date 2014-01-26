package consensus

import (
	"flag"
	"math/rand"
	"runtime"
	"time"
)


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

var _test_integration = flag.Bool("test.integration", false, "run the integration tests")
var _test_seed = flag.Int64("test.seed", 0, "the random seed to use")
var _test_queries = flag.Int("test.queries", 10000, "the number of queries to run")

func init() {
	flag.Parse()
}

type ConsensusIntegrationTest struct {
	baseReplicaTest
	random *rand.Rand
}

var _ = gocheck.Suite(&ConsensusIntegrationTest{})

func (s *ConsensusIntegrationTest) SetUpSuite(c *gocheck.C) {
	s.baseReplicaTest.SetUpSuite(c)
	if !*_test_integration {
		c.Skip("-integration not provided")
	}
	runtime.GOMAXPROCS(1)
}

func (s *ConsensusIntegrationTest) SetUpTest(c *gocheck.C) {
	var seed rand.Source
	if *_test_seed != 0 {
		seed = rand.NewSource(time.Now().Unix())
	} else {
		seed = rand.NewSource(*_test_seed)
	}
	s.random = rand.New(seed)

	c.Log("ConsensusIntegrationTest seeded with: ", seed)

	s.baseScopeTest.SetUpTest(c)
}

// tests the operation of an egalitarian paxos cluster
// without any communication failures between nodes
func (s *ConsensusIntegrationTest) TestSuccessCase(c *gocheck.C) {
	c.Log("Testing success case")
	for i:=0; i<*_test_queries; i++ {

	}
}

// tests the operation of an egalitarian paxos cluster
// with communication failures between nodes
func (s *ConsensusIntegrationTest) TestFailureCase(c *gocheck.C) {
	c.Log("Testing failure case")
	for i:=0; i<*_test_queries; i++ {

	}
}
