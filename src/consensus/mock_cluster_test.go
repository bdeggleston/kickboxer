package consensus

//import (
//	"encoding/json"
//	"flag"
//	"fmt"
//	"math/rand"
//	"os"
//	"runtime"
//	"runtime/pprof"
//	"sync"
//	"time"
//)
//
//import (
//	"github.com/cactus/go-statsd-client/statsd"
//	"launchpad.net/gocheck"
//)
//
//import (
//	"message"
//	"node"
//	"store"
//)
//
///*
//   long running integration tests. Each integration iteration should run on one core, with
//	inter-node communication running through a message broker, which will randomly add failure
//	scenarios. At the beginning of each iteration, the random number generator should be seeded
//	with a value that can be recorded, so failures can be later played back and debugged. The
//	mockNode keeps a log of all instructions it has been given. Message logs between nodes
//	should be regularly compared. If any message lists are not equal, or one is not a subset
//	of the other, an error has occurred, and needs to be debugged.
//		Failure Scenarios:
//			* Network partition
//			* Unresponsive node
//			* Delayed / Out of order messages
//			* Missing messages
//		Also to do:
//			* wrap timeout event creation in a method that can be mocked out so the
//				test runner can send out random, repeatable timeout events
// */
//
//var _test_integration = flag.Bool("test.integration", false, "run the integration tests")
//var _test_show_progress = flag.Bool("test.progress", false, "run the integration tests")
//var _test_seed = flag.Int64("test.seed", 0, "the random seed to use")
//var _test_queries = flag.Int("test.queries", 1000, "the number of queries to run")
//var _test_query_rate = flag.Int("test.rate", 20, "the number of queries per second")
//var _test_replicas = flag.Int("test.replicas", 5, "the number of replicas in the test cluster")
//var _test_concurrent_queries = flag.Int("test.concurrent", 10, "the number of concurrent queries to run")
//var _test_cpu_profile = flag.Bool("test.profile", false, "profile the integration test")
//
//func init() {
//	flag.Parse()
//}
//
//type opsMsgSendEvent struct {
//	node *mockNode
//	msg message.Message
//	reply chan opsMsgRecvEvent
//}
//
//type opsMsgRecvEvent struct {
//	msg message.Message
//	err error
//}
//
//type opsTimeoutEvent struct {
//	reply chan <- time.Time
//}
//
//type opsMsgBacklog struct {
//	event opsMsgSendEvent
//	delay uint32
//}
//
//type opsTimeoutBacklog struct {
//	event opsTimeoutEvent
//	delay uint32
//}
//
//type opsCtrl struct {
//	c *gocheck.C
//	nodes []*mockNode
//	random *rand.Rand
//	msgChan chan opsMsgSendEvent
//	timeoutChan chan opsTimeoutEvent
//	simulateFailures bool
//	completed int
//	stats statsd.Statter
//}
//
//func (c *opsCtrl) messageHandler(node *mockNode, msg message.Message) (message.Message, error) {
//	replyChan := make(chan opsMsgRecvEvent)
//	sendEvent := opsMsgSendEvent{
//		node: node,
//		msg: msg,
//		reply: replyChan,
//	}
//	c.msgChan <- sendEvent
//	logger.Debug("%T message put in channel\n", msg)
//	reply := <- replyChan
//	logger.Debug("%T reply received from channel\n", reply.msg)
//	return reply.msg, reply.err
//}
//
//func (c *opsCtrl) timeoutHandler(d time.Duration) <-chan time.Time {
//	ticks := make(chan time.Time, 2)
//	duration := time.After(d)
//	reply := make(chan time.Time)
//	c.timeoutChan <- opsTimeoutEvent{ticks}
//
//	go func(){
//		var t time.Time
//		select {
//		case t = <- ticks:
//			reply <- t
//		case t = <- duration:
//			fmt.Println("Real timeout")
//			reply <- t
//		}
//	}()
//	return reply
//}
//
//func (c *opsCtrl) reactor() {
//	var open bool
//	var msgEvnt opsMsgSendEvent
//	var timeEvnt opsTimeoutEvent
//
//	handleMessage := func(evt opsMsgSendEvent) {
//		msg, err := mockNodeDefaultMessageHandler(evt.node, evt.msg)
//		evt.reply <- opsMsgRecvEvent{msg:msg, err:err}
//	}
//	lastLen := 0
//
//	// all length variables refer to the number
//	// of reactor ticks
//
//	// ratio of dropped messages
//	dropRatio := uint32(150)
//	// ratio of delayed messages
//	delayRatio := uint32(150)
//	// number of ticks messages are delayed
//	delayLenBase := uint32(20)
//	delayLenRange := uint32(10)
//	// how often a partition occurs
//	partionRatio := uint32(500)
//	// how long a partition lasts
//	partionLenBase := uint32(40)
//	partionLenRange := uint32(20)
//	// how long before a timeout fires
//	timeoutLenBase := uint32(len(c.nodes) * 2) // default to a couple of cycles
//	timeoutLenRange := uint32(len(c.nodes) * 1) // default to a couple of cycles
//
//	partitionMap := make(map[node.NodeId] uint32)
//	_ = partitionMap
//
//	msgBacklog := make([]opsMsgBacklog, 0)
//	timeBacklog := make([]opsTimeoutBacklog, 0)
//
//	indexMap := make(map[node.NodeId]int)
//	for i, n := range c.nodes {
//		indexMap[n.id] = i
//	}
//
//	for i:=0; true; i++ {
//		select {
//		case msgEvnt, open = <-c.msgChan:
//			if !open {
//				panic("closed")
//				return
//			}
//			if c.simulateFailures {
//				nid := msgEvnt.node.id
//				if partitionMap[nid] > 0 {
//					logger.Debug("** Node %v is partitioned, skipping %T msg", indexMap[nid], msgEvnt.msg)
//					break
//				} else if (c.random.Uint32() % partionRatio) == 0 {
//					delay := partionLenBase
//					delay += (c.random.Uint32() % (partionLenRange * 2)) - partionLenRange
//					partitionMap[msgEvnt.node.id] = delay
////					for _, node := range c.nodes {
////						if node.id == msgEvnt.node.id {
////							for _, rnode := range node.cluster.nodes {
////								rnode.(*mockNode).partition = true
////							}
////						}
////					}
//					logger.Debug("** Beginning partition for node: %v for %v ticks", indexMap[nid], delay)
//					break
//				} else if (c.random.Uint32() % dropRatio) == 0 {
//					logger.Debug("** Dropping %T for node %v", msgEvnt.msg, indexMap[nid])
//					break
//				} else if (c.random.Uint32() % delayRatio) == 0 {
//					delay := delayLenBase
//					delay += (c.random.Uint32() % (delayLenRange * 2)) - delayLenRange
//					backLog := opsMsgBacklog{delay:delay, event:msgEvnt}
//					msgBacklog = append(msgBacklog, backLog)
//					logger.Debug("** Delaying %T for node: %v for %v ticks", msgEvnt.msg, indexMap[nid], delay)
//					break
//				} else {
//					go handleMessage(msgEvnt)
//					runtime.Gosched()
//					logger.Debug("++ Handling %T for node: %v", msgEvnt.msg, indexMap[nid])
//				}
//			} else {
//				nid := msgEvnt.node.id
//				go handleMessage(msgEvnt)
//				runtime.Gosched()
//				logger.Debug("++ Handling %T for node: %v", msgEvnt.msg, indexMap[nid])
//			}
//			runtime.Gosched()
//		case timeEvnt, open = <-c.timeoutChan:
//			if !open {
//				panic("closed")
//				return
//			}
//			delay := timeoutLenBase
//			delay += (c.random.Uint32() % (timeoutLenRange * 2)) - timeoutLenRange
//			backLog := opsTimeoutBacklog{delay:delay, event:timeEvnt}
//			timeBacklog = append(timeBacklog, backLog)
//			c.c.Logf("Handling timeout request")
//			runtime.Gosched()
//		case <- time.After(10 * time.Second):
//			panic("deadlock")
//		}
//		c.stats.Gauge("pending_messages", int64(len(c.msgChan)), 1.0)
//
//		drain: for {
//			select {
//			case msgEvnt, open = <-c.msgChan:
//				go handleMessage(msgEvnt)
//			default:
//				runtime.Gosched()
//				break drain
//			}
//
//		}
//
//		oldMsgBacklog := msgBacklog
//		msgBacklog = make([]opsMsgBacklog, 0)
//		for _, l := range oldMsgBacklog {
//			if l.delay <= 0 {
//				c.c.Logf("++ Handling delayed message %T for node: %v", l.event, l.event.node.id)
//				handleMessage(l.event)
//			} else {
//				l.delay--
//				msgBacklog = append(msgBacklog, l)
//			}
//		}
//
//		oldTimeBacklog := timeBacklog
//		timeBacklog = make([]opsTimeoutBacklog, 0)
//		for _, l := range oldTimeBacklog {
//			if l.delay < 0 {
//				c.c.Logf("Sending timeout event")
//				go func() { l.event.reply <- time.Now() }()
//				runtime.Gosched()
//			} else {
//				l.delay--
//				timeBacklog = append(timeBacklog, l)
//			}
//		}
//
//		for nid, remaining := range partitionMap {
//			if remaining <= 0 {
//				delete(partitionMap, nid)
//				c.c.Logf("Ending partition for node: %v", indexMap[nid])
////				for _, node := range c.nodes {
////					if nid == msgEvnt.node.id {
////						for _, rnode := range node.cluster.nodes {
////							rnode.(*mockNode).partition = false
////						}
////					}
////				}
//			} else {
//				partitionMap[nid]--
//			}
//
//		}
//
////		if i%50 != 0 {
////			continue
////		}
//
////		if i%50 != 0 {
////			continue
////		}
//
//		// lock all clusters while testing
//		for _, n := range c.nodes {
//			n.cluster.lock.Lock()
//		}
//
//		// check the store instructions,
//		highNode := 0
//		maxInstructions := 0
//		var instructionsSet []*store.Instruction
//		for i, node := range c.nodes {
//			cluster := node.cluster
//			if len(cluster.instructions) > maxInstructions {
//				maxInstructions = len(cluster.instructions)
//				highNode = i
//				instructionsSet = cluster.instructions
//			}
//		}
//
//		fmt.Println("Time: ", time.Now())
//		if *_test_show_progress {
//			lastLen = len(instructionsSet)
//			start := 0
//			if len(instructionsSet) > 30 {
//				start = len(instructionsSet) - 30
//			}
//			for i:=start; i<lastLen; i++ {
//				outStr := ""
//				for _, node := range c.nodes {
//					if len(node.cluster.instructions) < i + 1 {
//						outStr += " -- "
//					} else {
//						instruction := node.cluster.instructions[i]
//						outStr += fmt.Sprintf(" %v ", instruction.Args[0])
//					}
//				}
//				fmt.Println(outStr)
//			}
//		}
//
//		// check the order of instance execution
//		if true {
//			scopes := make([]*Scope, len(c.nodes))
//			for n, node := range c.nodes {
//				scopes[n] = node.manager.getScope("a")
//			}
//
//			var numInst []int
//			numInst = make([]int, len(c.nodes))
//			for x, scope := range scopes {
//				numInst[x] = scope.instances.Len()
//			}
//			fmt.Printf("%v <- known instances\n", numInst)
//
//			numInst = make([]int, len(c.nodes))
//			for x, scope := range scopes {
//				numInst[x] = scope.inProgress.Len()
//			}
//			fmt.Printf("%v <- in progress instances\n", numInst)
//
//			numInst = make([]int, len(c.nodes))
//			for x, scope := range scopes {
//				numInst[x] = scope.committed.Len()
//			}
//			completeInst := numInst
//			fmt.Printf("%v <- committed instances\n", numInst)
//
//			numInst = make([]int, len(c.nodes))
//			for x, scope := range scopes {
//				numInst[x] = len(scope.executed)
//				completeInst[x] += numInst[x]
//			}
//			fmt.Printf("%v <- executed instances\n", numInst)
//			fmt.Printf("%v <- completed instances\n", completeInst)
//			c.stats.Gauge("num_goroutines", int64(runtime.NumGoroutine()), 0.5)
//			fmt.Printf("Num Goroutines: %v\n", runtime.NumGoroutine())
//
//			committed := NewInstanceIDSet([]InstanceID{})
//			for _, scp := range scopes {
//				for _, iid := range scp.committed.InstanceIDs() {
//					committed.Add(iid)
//				}
//			}
//
//			for n, cnode := range c.nodes {
//				if n == highNode { continue }
//				for i, instruction := range cnode.cluster.instructions {
//					if i + 1 > len(instructionsSet) {
//						continue
//					}
//					// much faster than gocheck
//					if instruction.Args[0] != instructionsSet[i].Args[0] {
//
//						expectedScope := c.nodes[highNode].manager.getScope("a")
//						actualScope := c.nodes[n].manager.getScope("a")
//
//						divergenceIndex := 0
//						for divergenceIndex=0; i<len(actualScope.executed); divergenceIndex++ {
//							if divergenceIndex > len(actualScope.executed)-1 || divergenceIndex > len(expectedScope.executed)-1 {
//								break
//							}
//							if expectedScope.executed[divergenceIndex] != actualScope.executed[divergenceIndex] {
//								break
//							}
//						}
//
//						fmt.Println("expected / actual")
//
//						expectedInstances := make([]*Instance, 0)
//						for _, iid := range expectedScope.executed[divergenceIndex-1:] {
//							expectedInstances = append(expectedInstances, expectedScope.instances.Get(iid))
//						}
//						actualInstances := make([]*Instance, 0)
//						for _, iid := range actualScope.executed[divergenceIndex-1:] {
//							actualInstances = append(actualInstances, expectedScope.instances.Get(iid))
//						}
//
//						var js []byte
//						var err error
//						js, err = json.Marshal(expectedInstances)
//						fmt.Println(string(js))
//						fmt.Println(err)
////						fmt.Println(err.(*json.InvalidUTF8Error).S, err)
//						js, err = json.Marshal(actualInstances)
//						fmt.Println(string(js))
//						fmt.Println(err)
////						fmt.Println(err.(*json.InvalidUTF8Error).S, err)
//
//						var iid_pointer1 *InstanceID
//						var iid_pointer2 *InstanceID
//						for _, scope := range scopes {
//							if len(scope.executed) - 1 < divergenceIndex {
//								continue
//							}
//							iid := scope.executed[divergenceIndex]
//							if iid_pointer1 == nil {
//								iid_pointer1 = &iid
//							} else if iid != *iid_pointer1 {
//								iid_pointer2 = &iid
//							}
//						}
//						inst1Map := make(map[string]*Instance)
//						inst2Map := make(map[string]*Instance)
//
//						for _, scope := range scopes {
//							inst1Map[scope.GetLocalID().String()] = scope.instances.Get(*iid_pointer1)
//							inst2Map[scope.GetLocalID().String()] = scope.instances.Get(*iid_pointer2)
//						}
//						fmt.Println("")
//						js, err = json.Marshal(inst1Map)
//						fmt.Println(string(js))
//						fmt.Println(err)
//						js, err = json.Marshal(inst2Map)
//						fmt.Println(string(js))
//						fmt.Println(err)
//
//						scopes := make([]*Scope, len(c.nodes))
//						instances := make([]*InstanceMap, len(c.nodes))
//						inst0 := 0
//						for i, inode := range c.nodes {
//							instances[i] = inode.manager.getScope("a").instances
//							scopes[i] = inode.manager.getScope("a")
//						}
//
//						// confirm that all instance dependencies match
//						for i := range c.nodes {
//							if i == inst0 { continue }
//							for _, iid := range instances[inst0].InstanceIDs() {
//								expected := instances[inst0].Get(iid)
//								actual := instances[i].Get(iid)
//								if expected == nil || actual == nil {
//									continue
//								}
//								if expected.Status != INSTANCE_COMMITTED || actual.Status != INSTANCE_COMMITTED {
//									continue
//								}
//								if !NewInstanceIDSet(expected.Dependencies).Equal(NewInstanceIDSet(actual.Dependencies)) {
//									fmt.Printf("%v / %v mismatch\n", inst0, i)
//									fmt.Printf("%+v\n", instances[inst0].Get(iid).Dependencies)
//									fmt.Printf("%+v\n", instances[i].Get(iid).Dependencies)
//								}
//								if expected.Sequence != actual.Sequence {
//									fmt.Printf("%v / %v mismatch\n", inst0, i)
//									fmt.Printf("%+v\n", expected.Sequence)
//									fmt.Printf("%+v\n", actual.Sequence)
//								}
//							}
//						}
//
//						scope := cnode.manager.getScope("a")
//						localInst := scope.instances.Get(scope.executed[len(scope.executed) - 1])
//						//					fmt.Printf("Local:  %+v\n", localInst)
//
//						for _, onode := range c.nodes {
//							if onode.id == cnode.id {
//								continue
//							}
//							scope := onode.manager.getScope("a")
//							rInst := scope.instances.Get(localInst.InstanceID)
//							if !c.c.Check(NewInstanceIDSet(rInst.Dependencies).Equal(NewInstanceIDSet(localInst.Dependencies)), gocheck.Equals, true) {
//								fmt.Printf("%+v\n", NewInstanceIDSet(rInst.Dependencies))
//								fmt.Printf("%+v\n", NewInstanceIDSet(localInst.Dependencies))
//							} else {
//								fmt.Println("Dependencies match")
//							}
//						}
//						os.Exit(-1)
//					}
//				}
//			}
//
//		}
//
//		sizes := make([]int, len(c.nodes))
//		allGood := true
//		for i, node := range c.nodes {
//			sizes[i] = len(node.cluster.instructions)
//			allGood = allGood && sizes[i] == *_test_queries
//
//		}
//		logger.Debug("-- Instruction parity check ok [%v](%v)", len(instructionsSet), sizes)
//		if allGood {
//			c.c.Logf("All instructions processed")
//			return
//		}
//		c.completed = len(instructionsSet)
//
//		// unlock all clusters
//		for _, n := range c.nodes {
//			n.cluster.lock.Unlock()
//		}
//	}
//}
//
//// creates a new operations controller
//func newCtrl(r *rand.Rand, nodes []*mockNode, c *gocheck.C) *opsCtrl {
//	ctrl := &opsCtrl{
//		c: c,
//		random: r,
//		nodes: nodes,
//		msgChan: make(chan opsMsgSendEvent, 10000),  // should this be buffered?
//		timeoutChan: make(chan opsTimeoutEvent, 1000),  // should this be buffered?
//	}
//
//	for i, node := range nodes {
//		fmt.Println("Node", i, node.id)
//	}
//
//	// patch nodes
//	for _, node := range ctrl.nodes {
//		node.messageHandler = ctrl.messageHandler
//	}
//	// TODO: workout way to test noop commit
//	// TODO: test full partition (nothing in or out)
//	// TODO: setup a tick count / real time hybrid
////	consensusTimeoutEvent = ctrl.timeoutHandler
//	go ctrl.reactor()
//
//	return ctrl
//}
//
//type MockClusterIntegrationTest struct {
//	baseReplicaTest
//	ctrl *opsCtrl
//	random *rand.Rand
//	oldTimeoutHandler func(d time.Duration) <-chan time.Time
//	seedVal int64
//	stats statsd.Statter
//}
//
//var _ = gocheck.Suite(&MockClusterIntegrationTest{})
//
//func (s *MockClusterIntegrationTest) SetUpSuite(c *gocheck.C) {
//	s.baseReplicaTest.SetUpSuite(c)
//	s.numNodes = *_test_replicas
//	s.oldTimeoutHandler = consensusTimeoutEvent
//	if !*_test_integration {
//		c.Skip("-integration not provided")
//	}
//	runtime.GOMAXPROCS(1)
//
//	var err error
//	s.stats, err = statsd.New("localhost:8125", "integration.test")
//	if err != nil {
//		panic(err)
//	}
//}
//
//func (s *MockClusterIntegrationTest) TearDownSuite(c *gocheck.C) {
//	consensusTimeoutEvent = s.oldTimeoutHandler
//}
//
//func (s *MockClusterIntegrationTest) SetUpTest(c *gocheck.C) {
//	if *_test_seed != 0 {
//		s.seedVal = *_test_seed
//		c.Log("Using seed arg: ", s.seedVal)
//	} else {
//		s.seedVal = time.Now().Unix()
//	}
//	rand.Seed(s.seedVal)
//	s.random = rand.New(rand.NewSource(s.seedVal))
//
//	c.Log("MockClusterIntegrationTest seeded with: ", s.seedVal)
//
//	s.baseReplicaTest.SetUpTest(c)
//	s.ctrl = newCtrl(s.random, s.nodes, c)
//
//	nodeStats, err := statsd.New("localhost:8125", "integration.node")
//	if err != nil {
//		panic(err)
//	}
//	for i, n := range s.nodes {
//		var err error
//		_ = i
////		n.manager.stats, err = statsd.New("localhost:8125", fmt.Sprintf("node%v", i))
//		n.manager.stats, err = statsd.New("localhost:8125", "integration")
//		if err != nil {
//			panic(err)
//		}
//		n.stats = nodeStats
//		for _, r := range n.cluster.nodes {
//			r.(*mockNode).stats = nodeStats
//		}
//	}
//	s.stats.Inc("setup", 1, 1.0)
//	s.ctrl.stats, err = statsd.New("localhost:8125", "integration.ctrl")
//	if err != nil {
//		panic(err)
//	}
//}
//
//func (s *MockClusterIntegrationTest) runTest(c *gocheck.C) {
//	if *_test_cpu_profile {
//		fmt.Println("profiling")
//		m, err := os.Create("integration_test.mem.prof")
//		if err != nil { panic(err) }
//		f, err := os.Create("integration_test.prof")
//		if err != nil { panic(err) }
//		err = pprof.StartCPUProfile(f)
//		if err != nil { panic(err) }
//		defer func() {
//			pprof.WriteHeapProfile(m)
//			pprof.StopCPUProfile()
//			m.Close()
//			f.Close()
//		}()
//	}
////	oldPreacceptTimeout := PREACCEPT_TIMEOUT
////	PREACCEPT_TIMEOUT = PREACCEPT_TIMEOUT * 2
////	oldPreacceptCommitTimeout := PREACCEPT_COMMIT_TIMEOUT
////	PREACCEPT_COMMIT_TIMEOUT = PREACCEPT_COMMIT_TIMEOUT * 2
////	oldAcceptTimeout := ACCEPT_TIMEOUT
////	ACCEPT_TIMEOUT = ACCEPT_TIMEOUT * 2
////	oldAcceptCommitTimeout := ACCEPT_COMMIT_TIMEOUT
////	ACCEPT_COMMIT_TIMEOUT = ACCEPT_COMMIT_TIMEOUT * 2
////	oldPrepareTimeout := PREPARE_TIMEOUT
////	PREPARE_TIMEOUT = PREPARE_TIMEOUT * 2
////	oldPrepareCommitTimeout := PREPARE_COMMIT_TIMEOUT
////	PREPARE_COMMIT_TIMEOUT = PREPARE_COMMIT_TIMEOUT * 2
////	oldBallotFailureWaitTime := BALLOT_FAILURE_WAIT_TIME
////	BALLOT_FAILURE_WAIT_TIME = BALLOT_FAILURE_WAIT_TIME * 2
////	oldExecuteTimeout := EXECUTE_TIMEOUT
////	EXECUTE_TIMEOUT = EXECUTE_TIMEOUT * 2
////	oldSuccessorTimeout := SUCCESSOR_TIMEOUT
////	SUCCESSOR_TIMEOUT = SUCCESSOR_TIMEOUT * 2
////	defer func() {
////		PREACCEPT_TIMEOUT = oldPreacceptTimeout
////		PREACCEPT_COMMIT_TIMEOUT = oldPreacceptCommitTimeout
////		ACCEPT_TIMEOUT = oldAcceptTimeout
////		ACCEPT_COMMIT_TIMEOUT = oldAcceptCommitTimeout
////		PREPARE_TIMEOUT = oldPrepareTimeout
////		PREPARE_COMMIT_TIMEOUT = oldPrepareCommitTimeout
////		BALLOT_FAILURE_WAIT_TIME = oldBallotFailureWaitTime
////		EXECUTE_TIMEOUT = oldExecuteTimeout
////		SUCCESSOR_TIMEOUT = oldSuccessorTimeout
////	}()
//
//
////	semaphore := make(chan bool, *_test_concurrent_queries)
//	queryWait := time.Duration(1000 / *_test_query_rate) * time.Millisecond
//	numQueries := *_test_queries
//	wg := sync.WaitGroup{}
//	wg.Add(numQueries)
//	errors := 0
//	for i:=0; i<numQueries; i++ {
//		c.Logf("Iteration %v", i)
//		manager := s.nodes[s.random.Int() % len(s.nodes)].manager
//		instructions := []*store.Instruction{store.NewInstruction("set", "a", []string{fmt.Sprint(i)}, time.Now())}
//		s.stats.Gauge("query", int64(i), 1.0)
////		semaphore <- true
//		go func() {
//			queryStart := time.Now()
//			_, err := manager.ExecuteQuery(instructions)
//			if err != nil {
//				s.stats.Inc("query.failed", 1, 1.0)
//				fmt.Printf("FAILED QUERY: %v\n", err)
//				errors++
//			} else {
//				s.stats.Inc("query.success", 1, 1.0)
//				fmt.Println("QUERY COMPLETE")
//			}
////			<- semaphore
////			select {
////			case <- semaphore:
////				//
////			default:
////				//
////			}
//			queryEnd := time.Now()
//			delta := queryEnd.Sub(queryStart) / time.Millisecond
//			s.stats.Timing("query.time", int64(delta), 1.0)
//			wg.Done()
//		}()
//		time.Sleep(queryWait)
//	}
//	wg.Wait()
//	c.Logf("%v queries completed with %v failed client requests\n", numQueries, errors)
//	c.Logf("Test completed with seed: %v\n", s.seedVal)
//}
//
//// tests the operation of an egalitarian paxos cluster
//// without any communication failures between nodes
//func (s *MockClusterIntegrationTest) TestSuccessCase(c *gocheck.C) {
//	c.Log("Testing success case")
//	s.runTest(c)
//}
//
//// tests the operation of an egalitarian paxos cluster
//// with communication failures between nodes
//func (s *MockClusterIntegrationTest) TestFailureCase(c *gocheck.C) {
//	c.Log("Testing failure case")
//	s.ctrl.simulateFailures = true
//	s.runTest(c)
//}
