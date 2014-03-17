package consensus

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
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

func (s *ConsensusQueryBenchmarks) checkConsistency(c *gocheck.C) {

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

	waitTime := time.Second / time.Duration(*benchRate)

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
			key = s.keys[s.random.Int() % len(s.nodes)]
		} else {
			key = s.keys[0]
		}
		instructions := []*store.Instruction{store.NewInstruction("set", key, []string{fmt.Sprint(i)}, time.Now())}
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
		delta := queryEnd.Sub(queryStart) / time.Millisecond
		s.stats.Timing("query.time", int64(delta), 1.0)
		wg.Done()
	}

	for i:=0;i<numQueries;i++ {
		s.stats.Inc("query.request", 1, 1.0)
		go query(i)
		time.Sleep(waitTime)
	}
	wg.Wait()
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

	start := time.Now()
	s.runBenchmark(*benchQueries, c)
	end := time.Now()
	logger.Info("\nQuery time: %v\n", end.Sub(start))
	s.checkConsistency(c)
}

func BenchmarkQueries(b *testing.B) {
	cqb := ConsensusQueryBenchmarks{}
	cqb.SetUpSuite(nil)
	cqb.SetUpTest(nil)
	cqb.runBenchmark(b.N * 100, nil)
}

