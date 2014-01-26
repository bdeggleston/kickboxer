package consensus

import (
	"flag"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"
)


import (
	"launchpad.net/gocheck"
	"message"
	"node"
	"store"
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

type opsMsgSendEvent struct {
	node *mockNode
	msg message.Message
	reply chan opsMsgRecvEvent
}

type opsMsgRecvEvent struct {
	msg message.Message
	err error
}

type opsTimeoutEvent struct {
	reply chan <- time.Time
}

type opsMsgBacklog struct {
	event opsMsgSendEvent
	delay uint32
}

type opsTimeoutBacklog struct {
	event opsTimeoutEvent
	delay uint32
}

type opsCtrl struct {
	c *gocheck.C
	nodes []*mockNode
	random *rand.Rand
	msgChan chan opsMsgSendEvent
	timeoutChan chan opsTimeoutEvent
	simulateFailures bool
}

func (c *opsCtrl) messageHandler(node *mockNode, msg message.Message) (message.Message, error) {
	replyChan := make(chan opsMsgRecvEvent)
	sendEvent := opsMsgSendEvent{
		node: node,
		msg: msg,
		reply: replyChan,
	}
	c.msgChan <- sendEvent
	reply := <- replyChan
	return reply.msg, reply.err
}

func (c *opsCtrl) timeoutHandler(d time.Duration) <-chan time.Time {
	reply := make(chan time.Time)
	c.timeoutChan <- opsTimeoutEvent{reply}
	return reply
}

func (c *opsCtrl) reactor() {
	var open bool
	var msgEvnt opsMsgSendEvent
	var timeEvnt opsTimeoutEvent

	handleMessage := func(evt opsMsgSendEvent) {
		msg, err := mockNodeDefaultMessageHandler(evt.node, evt.msg)
		evt.reply <- opsMsgRecvEvent{msg:msg, err:err}
	}

	// all length variables refer to the number
	// of reactor ticks

	// ratio of dropped messages
	dropRatio := uint32(150)
	// ratio of delayed messages
	delayRatio := uint32(100)
	// number of ticks messages are delayed
	delayLenBase := uint32(20)
	delayLenRange := uint32(10)
	// how often a partition occurs
	partionRatio := uint32(1000)
	// how long a partition lasts
	partionLenBase := uint32(40)
	partionLenRange := uint32(20)
	// how long before a timeout fires
	timeoutLenBase := uint32(len(c.nodes) * 5) // default to a couple of cycles
	timeoutLenRange := uint32(len(c.nodes) * 5) // default to a couple of cycles

	partitionMap := make(map[node.NodeId] uint32)
	_ = partitionMap

	msgBacklog := make([]opsMsgBacklog, 0)
	timeBacklog := make([]opsTimeoutBacklog, 0)

	for i:=0; true; i++ {
		select {
		case msgEvnt, open = <-c.msgChan:
			if !open {
				panic("closed")
				return
			}
			if c.simulateFailures {
				nid := msgEvnt.node.id
				if partitionMap[nid] > 0 {
					c.c.Logf("Node %v is partitioned, skipping msg", nid)
					break
				} else if (c.random.Uint32() % partionRatio) == 0 {
					delay := partionLenBase
					delay += (c.random.Uint32() % (partionLenRange * 2)) - partionLenRange
					partitionMap[msgEvnt.node.id] = delay
					c.c.Logf("Beginning partition for node: %v for %v ticks", nid, delay)
					break
				} else if (c.random.Uint32() % dropRatio) == 0 {
					c.c.Logf("Dropping message for node %v", nid)
					break
				} else if (c.random.Uint32() % delayRatio) == 0 {
					delay := delayLenBase
					delay += (c.random.Uint32() % (delayLenRange * 2)) - delayLenRange
					backLog := opsMsgBacklog{delay:delay, event:msgEvnt}
					msgBacklog = append(msgBacklog, backLog)
					c.c.Logf("Delaying %T for node: %v for %v ticks", msgEvnt.msg, nid, delay)
					break
				} else {
					handleMessage(msgEvnt)
					c.c.Logf("Handling %T for node: %v", msgEvnt.msg, nid)
				}
			} else {
				nid := msgEvnt.node.id
				handleMessage(msgEvnt)
				c.c.Logf("Handling %T for node: %v", msgEvnt.msg, nid)
			}
			runtime.Gosched()
		case timeEvnt, open = <-c.timeoutChan:
			if !open {
				panic("closed")
				return
			}
			delay := timeoutLenBase
			delay += (c.random.Uint32() % (timeoutLenRange * 2)) - timeoutLenRange
			backLog := opsTimeoutBacklog{delay:delay, event:timeEvnt}
			timeBacklog = append(timeBacklog, backLog)
			c.c.Logf("Handling timeout request")
			runtime.Gosched()
		default:
			// don't just spin
			c.c.Log("No new events, yielding thread")
			time.Sleep(time.Duration(10) * time.Millisecond)
			runtime.Gosched()
		}

		oldMsgBacklog := msgBacklog
		msgBacklog = make([]opsMsgBacklog, 0)
		for _, l := range oldMsgBacklog {
			if l.delay <= 0 {
				handleMessage(l.event)
			} else {
				l.delay--
				msgBacklog = append(msgBacklog, l)
			}
		}

		oldTimeBacklog := timeBacklog
		timeBacklog = make([]opsTimeoutBacklog, 0)
		for _, l := range oldTimeBacklog {
			if l.delay <= 0 {
				c.c.Logf("Sending timeout event")
				go func() { l.event.reply <- time.Now() }()
			} else {
				l.delay--
				timeBacklog = append(timeBacklog, l)
			}
		}

		for nid, remaining := range partitionMap {
			if remaining <= 0 {
				delete(partitionMap, nid)
				c.c.Logf("Ending partition for node: %v", nid)
			} else {
				partitionMap[nid]--
			}

		}

		// check the store instructions,
		highNode := 0
		maxInstructions := 0
		var instructionsSet []*store.Instruction
		for i, node := range c.nodes {
			cluster := node.cluster
			if len(cluster.instructions) > maxInstructions {
				maxInstructions = len(cluster.instructions)
				highNode = i
				instructionsSet = cluster.instructions
			}
		}

		for n, node := range c.nodes {
			// check that there's at least a little parity
			if len(instructionsSet) > 10 {
				c.c.Assert(len(node.cluster.instructions) > (len(instructionsSet) / 2), gocheck.Equals, true)
			}
			if n == highNode { continue }
			for i, instruction := range node.cluster.instructions {
				c.c.Assert(instruction, gocheck.DeepEquals, instructionsSet[i])
			}
		}
		c.c.Logf("Instruction (%v) check ok", len(instructionsSet))
	}
}

// creates a new operations controller
func newCtrl(r *rand.Rand, nodes []*mockNode, c *gocheck.C) *opsCtrl {
	ctrl := &opsCtrl{
		c: c,
		random: r,
		nodes: nodes,
		msgChan: make(chan opsMsgSendEvent, 1000),  // should this be buffered?
		timeoutChan: make(chan opsTimeoutEvent, 1000),  // should this be buffered?
	}

	// patch nodes
	for _, node := range ctrl.nodes {
		node.messageHandler = ctrl.messageHandler
	}
	consensusTimeoutEvent = ctrl.timeoutHandler
	go ctrl.reactor()

	return ctrl
}

type ConsensusIntegrationTest struct {
	baseReplicaTest
	ctrl *opsCtrl
	random *rand.Rand
	oldTimeoutHandler func(d time.Duration) <-chan time.Time
}

var _ = gocheck.Suite(&ConsensusIntegrationTest{})

func (s *ConsensusIntegrationTest) SetUpSuite(c *gocheck.C) {
	s.baseReplicaTest.SetUpSuite(c)
	s.oldTimeoutHandler = consensusTimeoutEvent
	if !*_test_integration {
		c.Skip("-integration not provided")
	}
	runtime.GOMAXPROCS(1)
}

func (s *ConsensusIntegrationTest) TearDownSuite(c *gocheck.C) {
	consensusTimeoutEvent = s.oldTimeoutHandler
}

func (s *ConsensusIntegrationTest) SetUpTest(c *gocheck.C) {
	var seed rand.Source
	if *_test_seed != 0 {
		seed = rand.NewSource(time.Now().Unix())
	} else {
		seed = rand.NewSource(*_test_seed)
	}
	s.random = rand.New(seed)

	c.Log("ConsensusIntegrationTest seeded with: ", seed.Int63())

	s.baseReplicaTest.SetUpTest(c)
	s.ctrl = newCtrl(s.random, s.nodes, c)
}

// tests the operation of an egalitarian paxos cluster
// without any communication failures between nodes
func (s *ConsensusIntegrationTest) TestSuccessCase(c *gocheck.C) {
	c.Log("Testing success case")
	wg := sync.WaitGroup{}
	_ = wg
//	wg.Add(*_test_queries)
	for i:=0; i<*_test_queries; i++ {
		c.Logf("Iteration %v", i)
		manager := s.nodes[s.random.Int() % len(s.nodes)].manager
		instructions := []*store.Instruction{store.NewInstruction("set", "a", []string{fmt.Sprint(i)}, time.Now())}
		manager.ExecuteQuery(instructions)
//		go func() {
//			manager.ExecuteQuery(instructions)
//			wg.Done()
//			panic("!")
//		}()
	}
//	wg.Wait()
}

// tests the operation of an egalitarian paxos cluster
// with communication failures between nodes
func (s *ConsensusIntegrationTest) TestFailureCase(c *gocheck.C) {
	c.Log("Testing failure case")
	s.ctrl.simulateFailures = true
	for i:=0; i<*_test_queries; i++ {
		manager := s.nodes[s.random.Int() % len(s.nodes)].manager
		instructions := []*store.Instruction{store.NewInstruction("set", "a", []string{fmt.Sprint(i)}, time.Now())}
		manager.ExecuteQuery(instructions)
	}
}
