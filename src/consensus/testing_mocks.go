package consensus

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

import (
	"github.com/cactus/go-statsd-client/statsd"
)

import (
	"message"
	"node"
	"partitioner"
	"store"
	"topology"
)

type intVal struct {
	value int
	time time.Time
}

func newIntVal(val int, ts time.Time) *intVal {
	return &intVal{value:val, time:ts}
}

func (v *intVal) GetTimestamp() time.Time { return v.time }
func (v *intVal) GetValueType() store.ValueType { return store.ValueType("INT") }

func (v *intVal) Equal(value store.Value) bool {
	if val, ok := value.(*intVal); ok {
		return val.value == v.value
	}
	return false
}

func (v *intVal) Serialize(_ *bufio.Writer) error { return nil }
func (v *intVal) Deserialize(_ *bufio.Reader) error { return nil }

func mockNodeDefaultMessageHandler(mn *mockNode, msg message.Message) (message.Message, error) {
	return mn.manager.HandleMessage(msg)
}

type mockNode struct {
	id node.NodeId
	dcID topology.DatacenterID
	token partitioner.Token
	name string

	// tracks the queries executed
	// against this node
	queries []*store.Instruction

	manager        *Manager
	messageHandler func(*mockNode, message.Message) (message.Message, error)
	sentMessages   []message.Message
	lock		   sync.Mutex
	partition     	bool
	stats			statsd.Statter
	started		bool
	status topology.NodeStatus
}

var _ topology.Node = &mockNode{}

// creates a simple node
// don't use for tests that depend on
// complex topology
func newMockNode() *mockNode {
	nid := node.NewNodeId()
	dcid := topology.DatacenterID("DC1")
	n := &mockNode{
		// TODO: generate random id
		id:             nid,
		queries:        []*store.Instruction{},
		messageHandler: mockNodeDefaultMessageHandler,
		sentMessages:   make([]message.Message, 0),
		stats:			newMockStatter(),

		started: true,

		dcID: dcid,
		token: partitioner.Token([]byte{0,0,0,0}),
		status: topology.NODE_UP,
		name: "node",
	}
	t := topology.NewTopology(nid, dcid, partitioner.NewMD5Partitioner(), 3)
	n.manager = NewManager(t, newMockStore())

	return n
}

func (n *mockNode) GetId() node.NodeId { return n.id }

func (n *mockNode) ExecuteQuery(cmd string, key string, args []string, timestamp time.Time) (store.Value, error) {
	store.NewInstruction(cmd, key, args, timestamp)
	return nil, nil
}

func (n *mockNode) SendMessage(srcRequest message.Message) (message.Message, error) {
	var err error
	var start time.Time

	getDuration := func(s time.Time) int64 {
		e := time.Now()
		delta := e.Sub(s) / time.Millisecond
		return int64(delta) + 1
	}

	buf := &bytes.Buffer{}
	if n.partition {
		logger.Debug("Skipping sent message from partitioned node")
		return nil, fmt.Errorf("Partition")
	}
	start = time.Now()
	err = message.WriteMessage(buf, srcRequest)
	n.stats.Timing(
		fmt.Sprintf("serialize.%T.time", srcRequest),
		getDuration(start),
		1.0,
	)
	n.stats.Inc(fmt.Sprintf("serialize.%T.count", srcRequest), 1, 1.0)

	if err != nil {
		return nil, err
	}
	start = time.Now()
	dstRequest, err := message.ReadMessage(buf)
	n.stats.Timing(
		fmt.Sprintf("deserialize.%T.time", dstRequest),
		getDuration(start),
		1.0,
	)
	n.stats.Inc(fmt.Sprintf("deserialize.%T.count", dstRequest), 1, 1.0)

	if err != nil {
		return nil, err
	}
	n.sentMessages = append(n.sentMessages, dstRequest)
	start = time.Now()
	srcResponse, err := n.messageHandler(n, dstRequest)
	n.stats.Timing(
		fmt.Sprintf("process.%T.time", srcResponse),
		getDuration(start),
		1.0,
	)
	n.stats.Inc(fmt.Sprintf("process.%T.count", srcResponse), 1, 1.0)

	if err != nil {
		n.stats.Inc(
			fmt.Sprintf("error.%T.time", srcRequest),
			1,
			1.0,
		)
		n.stats.Inc(fmt.Sprintf("error.%T.count", srcRequest), 1, 1.0)
		return nil, err
	}
	buf.Reset()
	start = time.Now()
	err = message.WriteMessage(buf, srcResponse)
	n.stats.Timing(
		fmt.Sprintf("serialize.%T.time", srcResponse),
		getDuration(start),
		1.0,
	)
	n.stats.Inc(fmt.Sprintf("serialize.%T.count", srcResponse), 1, 1.0)
	if err != nil {
		return nil, err
	}
	logger.Debug("Response size: %v\n", len(buf.Bytes()))
	start = time.Now()
	dstResponse, err := message.ReadMessage(buf)
	n.stats.Timing(
		fmt.Sprintf("deserialize.%T.time", dstResponse),
		getDuration(start),
		1.0,
	)
	n.stats.Inc(fmt.Sprintf("deserialize.%T.count", dstResponse), 1, 1.0)

	if err != nil {
		return nil, err
	}
	return dstResponse, nil
}

func (n *mockNode) Name() string { return n.name }
func (n *mockNode) GetAddr() string { return "" }
func (n *mockNode) IsStarted() bool { return n.started }
func (n *mockNode) GetToken() partitioner.Token { return n.token }
func (n *mockNode) GetDatacenterId() topology.DatacenterID { return n.dcID }
func (n *mockNode) GetStatus() topology.NodeStatus { return n.status }

func (n *mockNode) Start() error {
	n.started = true
	return nil
}

func (n *mockNode) Stop() error {
	n.started = false;
	return nil
}

func transformMockNodeArray(src []*mockNode) []node.Node {
	dst := make([]node.Node, len(src))
	for i := range src {
		dst[i] = node.Node(src[i])
	}
	return dst
}

// implements the statter interface
// used for testing things were called internally
// guages and timers only keep the most recent value
type mockStatter struct {
	mutex sync.RWMutex
	counters map[string]int64
	timers map[string]int64
	guages map[string]int64
}

var _ statsd.Statter = &mockStatter{}

func newMockStatter() *mockStatter {
	return &mockStatter{
		counters: make(map[string]int64),
		timers: make(map[string]int64),
		guages: make(map[string]int64),
	}
}

func (s *mockStatter) Inc(stat string, value int64, rate float32) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.counters[stat] += value
	return nil
}

func (s *mockStatter) Dec(stat string, value int64, rate float32) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.counters[stat] -= value
	return nil
}

func (s *mockStatter) Gauge(stat string, value int64, rate float32) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.guages[stat] = value
	return nil
}

func (s *mockStatter) GaugeDelta(stat string, value int64, rate float32) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.guages[stat] += value
	return nil
}

func (s *mockStatter) Timing(stat string, delta int64, rate float32) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.timers[stat] = delta
	return nil
}

func (s *mockStatter) SetPrefix(prefix string) {
}

func (s *mockStatter) Close() error {
	return nil
}

type mockStore struct {
	lock sync.Mutex
	instructions []store.Instruction
	values map[string]*intVal
}

func newMockStore() *mockStore {
	return &mockStore{
		instructions: make([]store.Instruction, 0),
		values: make(map[string]*intVal),
	}
}

var _ store.Store = &mockStore{}

func (s *mockStore) Start() error { return nil }
func (s *mockStore) Stop() error { return nil }

func (s *mockStore) ExecuteQuery(cmd string, key string, args []string, timestamp time.Time) (store.Value, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	intVal, err := strconv.Atoi(args[0])
	if err != nil { return nil, err }
	val := newIntVal(intVal, timestamp)
	s.values[key] = val
	s.instructions = append(s.instructions, store.NewInstruction(cmd, key, args, timestamp))
	return val, nil
}

func mockStoreDefaultInterferingKeys(c *mockStore, instruction store.Instruction) []string {
	return strings.Split(instruction.Key, ":")
}

func (s *mockStore) InterferingKeys(instruction store.Instruction) []string {
	return mockStoreDefaultInterferingKeys(s, instruction)
}

func mockStoreDefaultIsReadOnly(s *mockStore, instruction store.Instruction) bool {
	return false
}

func (s *mockStore) IsReadOnly(instruction store.Instruction) bool {
	return mockStoreDefaultIsReadOnly(s, instruction)
}

func mockStoreDefaultIsWriteOnly(s *mockStore, instruction store.Instruction) bool {
	return false
}

func (s *mockStore) IsWriteOnly(instruction store.Instruction) bool {
	return mockStoreDefaultIsWriteOnly(s, instruction)
}

// not implemented
func (s *mockStore) Reconcile(key string, values []store.Value) (store.Value, [][]store.Instruction, error) { panic("not implemented") }
func (s *mockStore) SerializeValue(v store.Value) ([]byte, error) { panic("not implemented") }
func (s *mockStore) DeserializeValue(b []byte) (store.Value, store.ValueType, error) { panic("not implemented") }
func (s *mockStore) GetRawKey(key string) (store.Value, error) { panic("not implemented") }
func (s *mockStore) SetRawKey(key string, val store.Value) error { panic("not implemented") }
func (s *mockStore) GetKeys() []string { panic("not implemented") }
func (s *mockStore) KeyExists(key string) bool { panic("not implemented") }
