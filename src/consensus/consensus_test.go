package consensus

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"time"
	"testing"
)

import (
	"launchpad.net/gocheck"
	"github.com/cactus/go-statsd-client/statsd"
)

import (
	"message"
	"store"
)

var (
	benchActive = flag.Bool("bench.active", false, "determines if the benchmark tests will be run")
	benchQueries = flag.Int("bench.queries", 1000, "number of queries to run")
	benchRate = flag.Int("bench.rate", 50, "Number of queries to run per second")
	benchReplicas = flag.Int("bench.replicas", 5, "sets number of replicas to run")
	benchThreaded = flag.Bool("bench.threaded", false, "runs test on multiple cores if set")
	benchSeed = flag.Int64("bench.seed", 0, "The random seed")
	benchStatsd = flag.Bool("bench.statsd", false, "sends metrics to statsd if set")
	benchLatency = flag.Int64("bench.latency", 0, "one way message delay in microseconds")
	benchProfile = flag.Bool("bench.profile", false, "activates performance profiling")
	benchNumKeys = flag.Int("bench.numkeys", 1, "activates performance profiling")
	benchMaster = flag.Bool("bench.master", false, "only executes queries against a single node")
)

type durationSorter struct {
	durations []time.Duration
}

func (d *durationSorter) Len() int {
	return len(d.durations)
}

func (d *durationSorter) Less(x, y int) bool {
	return d.durations[x] < d.durations[y]
}

func (d *durationSorter) Swap(x, y int) {
	d.durations[x], d.durations[y] = d.durations[y], d.durations[x]
}

type ConsensusQueryBenchmarks struct {
	baseReplicaTest

	random *rand.Rand
	seedVal int64
	stats statsd.Statter
	latency time.Duration
	keys []string
}

var _ = gocheck.Suite(&ConsensusQueryBenchmarks{})

func (s *ConsensusQueryBenchmarks) SetUpSuite(c *gocheck.C) {
	PAXOS_DEBUG = true
	s.baseReplicaTest.SetUpSuite(c)
	s.numNodes = *benchReplicas

	if *benchSeed != 0 {
		s.seedVal = *benchSeed
	} else {
		s.seedVal = time.Now().Unix()
	}
	s.random = rand.New(rand.NewSource(s.seedVal))


	s.latency = time.Microsecond * time.Duration(*benchLatency)

	// setup stats reporting
	var err error
	s.stats, err = statsd.New("localhost:8125", "integration.test")
	if err != nil {
		panic(err)
	}

	if *benchNumKeys < 1 {
		*benchNumKeys = 1
	}

	if *benchNumKeys == 1 {
		s.keys = []string{"a"}
	} else {
		s.keys = make([]string, *benchNumKeys)
		for i:=0; i<*benchNumKeys; i++ {
			s.keys[i] = fmt.Sprint(i)
		}
	}
}

func (s *ConsensusQueryBenchmarks) SetUpTest(c *gocheck.C) {
	s.baseReplicaTest.SetUpTest(c)

	// setup message handler
	for _, n := range s.nodes {
		n.messageHandler = s.messageHandler
		for _, r := range n.cluster.nodes {
			r.(*mockNode).messageHandler = s.messageHandler
		}
	}

	if *benchStatsd {
		c.Logf("instrumenting %v nodes...\n", len(s.nodes))
		nodeStats, err := statsd.New("localhost:8125", "integration.node")
		if err != nil {
			panic(err)
		}

		for _, n := range s.nodes {
			var err error
			n.manager.stats, err = statsd.New("localhost:8125", "integration")
			if err != nil {
				panic(err)
			}
			n.stats = nodeStats
			for _, r := range n.cluster.nodes {
				r.(*mockNode).stats = nodeStats
			}
		}
	} else {
		for _, n := range s.nodes {
			n.manager.stats, _ = statsd.NewNoop()
			for _, r := range n.cluster.nodes {
				r.(*mockNode).stats, _ = statsd.NewNoop()
			}
		}
	}
}

func (s *ConsensusQueryBenchmarks) messageHandler(mn *mockNode, msg message.Message) (message.Message, error) {
	if s.latency > 0 {
		time.Sleep(s.latency)
	}
	response, err := mn.manager.HandleMessage(msg)
	if s.latency > 0 {
		time.Sleep(s.latency)
	}
	return response, err
}

// checks that all of the queries were executed in the same order, per key
func (s *ConsensusQueryBenchmarks) checkConsistency(c *gocheck.C) {
	for _, n := range s.nodes {
		n.cluster.lock.Lock()
	}
	defer func() {
		for _, n := range s.nodes {
			n.cluster.lock.Unlock()
		}
	}()
	nodeInstructions := make([]map[string][]*Instance, s.numNodes)
	getKey := func(inst *Instance) string {
		return inst.Command.Key
	}
	getVal := func(inst *Instance) string {
		return inst.Command.Args[0]
	}
	for i, n := range s.nodes {

		imap := make(map[string][]*Instance)
		for _, iid := range n.manager.executed {
			instance := n.manager.instances.Get(iid)
			instructions := imap[getKey(instance)]
			if instructions == nil {
				instructions = make([]*Instance, 0, *benchQueries / *benchNumKeys)
			}
			instructions = append(instructions, instance)
			imap[getKey(instance)] = instructions
		}
		nodeInstructions[i] = imap
	}

	badKeys := 0

	checkKeys: for _, key := range s.keys {
		maxIdx := 0
		maxNumInstances := 0
		for i, imap := range nodeInstructions {
			if imap[key] == nil {
				imap[key] = make([]*Instance, 0)
			}
			if len(imap[key]) > maxNumInstances {
				maxIdx = i
				maxNumInstances = len(imap[key])
			}
		}
		mInstances := nodeInstructions[maxIdx][key]
		for i, imap := range nodeInstructions {
			if i == maxIdx {
				continue
			}
			instances := imap[key]
			for i:=0; i<len(instances); i++ {
				expected := mInstances[i]
				actual := instances[i]
				if !c.Check(expected.InstanceID, gocheck.Equals, actual.InstanceID,
					gocheck.Commentf("%v: %v %v != %v", key, i, getVal(expected), getVal(actual)),
				) {
					badKeys++
					fmt.Printf("Mismatched Instances for %v at %v: %v != %v\b", key, i, getVal(expected), getVal(actual))

					// print jsonified instances
					divergeIdx := i
					displayStart := divergeIdx - 2
					if displayStart < 0 {
						displayStart = 0
					}
					displayEnd := divergeIdx + 2
					expectedInstances := make([]*Instance, 0)
					actualInstances := make([]*Instance, 0)
					for j:=displayStart; j<=displayEnd; j++ {
						if j < len(mInstances) {
							expectedInstances = append(expectedInstances, mInstances[j])
						}
						if j < len(instances) {
							actualInstances = append(actualInstances, instances[j])
						}
					}
					var js []byte
					var err error
					fmt.Println("")
					js, err = json.Marshal(expectedInstances)
					if err != nil {
						panic(err)
					}
					fmt.Println("expected")
					fmt.Println(string(js))

					fmt.Println("")
					js, err = json.Marshal(actualInstances)
					if err != nil {
						panic(err)
					}
					fmt.Println("actual")
					fmt.Println(string(js))

					// export full execution history for key, for each node
					for i, nodeKeyInstructions := range nodeInstructions {
						js, err = json.Marshal(nodeKeyInstructions[key])
						if err != nil {
							panic(err)
						}
						f, err := os.Create(fmt.Sprintf("%v:%v.json", key, i))
						_, err = f.Write(js)
						if err != nil {
							panic(err)
						}
						err = f.Close()
						if err != nil {
							panic(err)
						}
					}

					e0 := make([]*Instance, len(s.nodes))
					e1 := make([]*Instance, len(s.nodes))
					for i, n := range s.nodes {
						e0[i] = n.manager.instances.Get(expected.InstanceID)
						e1[i] = n.manager.instances.Get(actual.InstanceID)
					}

					fmt.Println("")
					js, err = json.Marshal(e0)
					if err != nil {
						panic(err)
					}
					fmt.Println("e0")
					fmt.Println(string(js))

					fmt.Println("")
					js, err = json.Marshal(e1)
					if err != nil {
						panic(err)
					}
					fmt.Println("e1")
					fmt.Println(string(js))

					continue checkKeys
				}
			}
		}
	}
	if badKeys > 0 {
		fmt.Println("Bad Keys: ", badKeys)
	}
}

func (s *ConsensusQueryBenchmarks) runBenchmark(numQueries int, c *gocheck.C) {
	if *benchProfile {
		fmt.Println("profiling...\n")
		m, err := os.Create("mem.prof")
		if err != nil { panic(err) }
		f, err := os.Create("cpu.prof")
		if err != nil { panic(err) }
		err = pprof.StartCPUProfile(f)
		if err != nil { panic(err) }
		defer func() {
			pprof.WriteHeapProfile(m)
			pprof.StopCPUProfile()
			m.Close()
			f.Close()
		}()
	}

	wg := sync.WaitGroup{}
	wg.Add(numQueries)
	errors := 0

	numAccept := 0
	numPrepare := 0

	oldAcceptPhase := managerAcceptPhase
	managerAcceptPhase = func(m *Manager, instance *Instance) error {
		numAccept++
		return oldAcceptPhase(m, instance)
	}

	oldPreparePhase := managerPreparePhase
	managerPreparePhase = func(m *Manager, instance *Instance) error {
		numPrepare++
		fmt.Println("\nPrepare: ", instance.InstanceID)
		return oldPreparePhase(m, instance)
	}

	defer func() {
		managerAcceptPhase = oldAcceptPhase
		managerPreparePhase = oldPreparePhase
	}()


	waitTime := time.Second / time.Duration(*benchRate)
	queryTimes := make([]time.Duration, numQueries)

	query := func(i int) {
		queryStart := time.Now()
		var manager *Manager
		if *benchMaster {
			manager = s.nodes[0].manager
		} else {
			manager = s.nodes[s.random.Int() % len(s.nodes)].manager
		}
		var key string
		if len(s.keys) > 1 {
			key = s.keys[s.random.Int() % len(s.keys)]
		} else {
			key = s.keys[0]
		}
		instructions := store.NewInstruction("set", key, []string{fmt.Sprint(i)}, time.Now())
		_, err := manager.ExecuteQuery(instructions)
		if err != nil {
			s.stats.Inc("query.failed", 1, 1.0)
			logger.Info("FAILED QUERY: %v\n", err)
			errors++
		} else {
			s.stats.Inc("query.success", 1, 1.0)
			logger.Debug("Finished Query QUERY: %v\n", i)
		}
		queryEnd := time.Now()
		queryTimes[i] = queryEnd.Sub(queryStart)
		delta := queryEnd.Sub(queryStart) / time.Millisecond
		s.stats.Timing("query.time", int64(delta), 1.0)
		wg.Done()
	}

	fmt.Printf("Wait time: %v\n", waitTime)
	start := time.Now()

	// print a status update every n queries
	statusUpdateInterval := numQueries / 10
	statusUpdateInterval2 := numQueries / 100
	for i:=0; i<numQueries; i++ {
		// adjust wait time for drift
		expectedTime := start.Add(waitTime * time.Duration(i))
		actualTime := time.Now()
		localWait := waitTime - actualTime.Sub(expectedTime)
		if localWait < waitTime / time.Duration(4) {
			localWait = waitTime / time.Duration(4)
		}

		s.stats.Inc("query.request", 1, 1.0)
		go query(i)
		if i < numQueries - 1 {
			time.Sleep(localWait)
		}
		if i % statusUpdateInterval == 0 {
			fmt.Printf("\nExecuted query %v ", i)
		} else if i % statusUpdateInterval2 == 0 {
			fmt.Printf(".")
		}
	}
	queryEnd := time.Now()
	fmt.Printf("\n%v queries completed\n", numQueries)

	fmt.Println("")
	fmt.Printf("query dispatch time: %v\n", queryEnd.Sub(start))
	// this should be immediately after start + (waitTime * numQueries)
	fmt.Printf("query dispatch drift: %v\n", queryEnd.Sub(start) - (waitTime * time.Duration(numQueries)))

	// wait for queries to be completed
	wg.Wait()
	end := time.Now()
	fmt.Println("")
	fmt.Printf("Query time: %v\n", end.Sub(start))
	fmt.Printf("Query wrap up time: %v\n", end.Sub(queryEnd))

	// GC stats
	memStats := &runtime.MemStats{}
	runtime.ReadMemStats(memStats)
	gcTime := time.Duration(memStats.PauseTotalNs) * time.Nanosecond
	fmt.Printf("GC time: %v\n", gcTime)
	fmt.Printf("GC count: %v\n", memStats.NumGC)
	fmt.Printf("GC avg pause: %v\n", gcTime / time.Duration(memStats.NumGC))

	// get query time stats
	dSorter := &durationSorter{durations:queryTimes}
	sort.Sort(dSorter)
	fmt.Println("")
	fmt.Printf("Min query time: %v\n", queryTimes[0])
	fmt.Printf("Max query time: %v\n", queryTimes[len(queryTimes) - 1])
	fmt.Printf("Median query time: %v\n", queryTimes[len(queryTimes) / 2])
	var queryTimeSum time.Duration
	for _, qt := range queryTimes {
		queryTimeSum += qt
	}
	fmt.Printf("Mean query time: %v\n", queryTimeSum / time.Duration(len(queryTimes)))

	// extra phase stats
	fmt.Println("")
	fmt.Printf("Num Accept Phases : ~%v / %v\n", numAccept, numQueries)
	fmt.Printf("Num Prepare Phases : ~%v / %v\n", numPrepare, numQueries)
	fmt.Println("")

}

func (s *ConsensusQueryBenchmarks) TestBenchmarkQueries(c *gocheck.C) {
	if !*benchActive {
		c.Skip("-bench.active not set")
	}

	if *benchThreaded {
		runtime.GOMAXPROCS(runtime.NumCPU())
	} else {
		runtime.GOMAXPROCS(1)
	}

	s.runBenchmark(*benchQueries, c)

	s.checkConsistency(c)
}

func BenchmarkQueries(b *testing.B) {
	cqb := ConsensusQueryBenchmarks{}
	cqb.SetUpSuite(nil)
	cqb.SetUpTest(nil)
	cqb.runBenchmark(b.N * 100, nil)
}

