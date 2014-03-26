package consensus

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

import (
	"github.com/cactus/go-statsd-client/statsd"
)

import (
	cluster "clusterproto"
	"message"
	"node"
	"store"
)

var (
	// timeout receiving a quorum of
	// preaccept responses
	PREACCEPT_TIMEOUT = uint64(2000)

	// timeout receiving a quorum of
	// accept responses
	ACCEPT_TIMEOUT = uint64(2000)

	// the amount of time a replica will wait
	// on a message with a preaccept status
	// before attempting to force a commit
	PREACCEPT_COMMIT_TIMEOUT = PREACCEPT_TIMEOUT + ACCEPT_TIMEOUT + uint64(50)

	// the amount of time a replica will wait
	// on a message with a accept status
	// before attempting to force a commit
	ACCEPT_COMMIT_TIMEOUT = ACCEPT_TIMEOUT + uint64(50)

	// timeout receiving a quorum of
	// prepare responses
	PREPARE_TIMEOUT = uint64(2000)

	// the amount of time a replica will wait
	// on a message with a an attempted prepare
	// before attempting to force a commit again
	PREPARE_COMMIT_TIMEOUT = PREPARE_TIMEOUT + (PREPARE_TIMEOUT / 2)

	// the amount of time a node will wait on a
	// successor response before timing out
	SUCCESSOR_TIMEOUT = uint64(2000)

	// how often a node should check that the
	// successor is up while waiting on a prepare phase
	SUCCESSOR_CONTACT_INTERVAL = uint64(1000)

	// wait period between retrying operations
	// that failed due to ballot failures
	BALLOT_FAILURE_WAIT_TIME = uint64(250)

	// number of times an operation, which failed
	// due to an out of date ballot, will be retried
	// before the request fails
	BALLOT_FAILURE_RETRIES = 4

	// the amount of time other goroutines will
	// wait for the local leader goroutine to
	// execute it's committed instance before it's
	// assumed to have failed, and they execute it
	EXECUTE_TIMEOUT = uint64(500)

	STATS_SAMPLE_RATE = float32(0.1)
)

/*

TODO: support instances with multiple instructions, operating on different keys, owned by multiple nodes
	* will need to determine dependencies from different nodes, and track the status of
		dependencies it doesn't own (weird / hard / error prone)
	* this will be a much bigger job than anticipated, should factor out support for multiple
		instruction instances for now

TODO: add and test ballot checking to every message handler
TODO: add a 'passive' flag to executeInstance, that will execute if possible, but not prepare
TODO: instead of jumping into a prepare, the preparer, or successor should first check if the leader is still 'working' on the query

TODO: consider batching up queries into a single instance

TODO: make execute query tolerant of prepare phases updating the status of it's instance

TODO: fix all tests that take >100ms
TODO: remove commit timeouts, replace with last activity (last message sent received)

Prepare successors should increment the last activity time outs by their order in the successor list. The
farther they are from first successor, the higher their timeout should be

TODO: make the prepare phase a bit smarter

Prepare phase improvement:
If the prepare responses are all preaccepted instances with the dependency match flag set, prepare should send an accept
If the prepare responses are all preaccepted instances with the same ballot, the dependencies should be merged and an
	accept message should be sent.
If the prepare responses are all accepted instances with the same ballot, seq, and deps, a commit message should be sent
If any response has a committed or executed messages, a commit message should be sent to all replicas

TODO: work out a way to prioritize the completion of existing commands vs the processing of new ones

Keep a count of instances in progress, and delay new queries proportionally

TODO: think about potential race conditions caused by multiple goroutines running executeDependencyChain concurrently

------ older notes ------

1) The manager needs to know the consistency level, and have a means of querying the cluster
	for the proper replicas each time it needs to send a message to the replicas. If a replica
	is added to, or removed from, the cluster mid transaction, the transaction will be executing
	against an out of date set of replicas.

2) manager state persistence. Maybe add system state mutation method to store?
	A triply nested hash table should do the trick for most applications.
		ex:
			consensus:
				<consensus_state> [manager_id][instance_id] = serialized_instance
			cluster state:
				<cluster_state> [node_id][node_property] = node_metadata

3) Workout a method for removing old nodes from the dependency graph

4) Add response expected param to execute instance, so queries that don't expect
	a return value can return even if it's instance dependencies have not been committed

 */

/*
Notes:

Explicit Prepare:
	Prepare race condition. Since replicas have a common commit timeout, in the event of
	a leader failure, it's likely that several will try to to take control of an instance
	once it's commit timeout is up. In the worst case, each replica will increment it's
	ballot and send out prepare responses at the same time, and then rejecting the other
	prepare responses. with no replica successfully taking control of the instance. This
	could conceivably result in the prepare process being deadlocked.

	Possible solutions:
		add some jitter into the commit timeout
		on instance creation, the leader randomly sets an order of succession for prepare phase
			problems:
				if the immediate successor(s) fails, the prepare phase would be delayed

Cluster Changes:
	Joining nodes. When a node joins the cluster, it needs to get a snapshot of the data
	for a key, the id of the last executed instance, as well as the instance set for that
	key's consensus manager. That should allow it to start participating in the consensus
	process without any inconsistencies. Nodes should probably forward consensus state to
	the joining node while it's in the process of joining, since it probably wouldn't make
	sense to have a half joined node start participating in consensus for some of it's keys,
	but not for others.

Variable datacenter consistency:
	Should it even be allowed? If it were, we'd have a situation where different datacenters
	could diverge in their executed command history for queries without inter dc consistency,
	and then have this unresolvable situation when queries were done *with* inter dc
	consistency.
		Solutions:
			caveat emptor: Use interdc consistency. You can use local consistency if you want
				but any inconsistencies are on you. Probably a bad idea?
			force interdc consistency: don't allow local consensus. Kind of restrictive, but
				may be desirable in situations where consistency is the priority
			home dc: assign keys a 'home' datacenter. Probably the first dc to touch the key,
				unless explicity assigned. Queries performed in the home dc only run consensus
				in the local dcs, and remote dc nodes forward their queries to the home dc. The
				home dc could be moved automatically based on query frequency. Reassigning a manager's
				home would be an inter dc consensus operation.
				Local consensus reads can be performed against the local cluster, with the understanding
				that local consensus is not as consistent as inter dc consensus, and local consensus
				writes are forwarded to the home dc. Interdc consistency from any datacenter, must
				hear from a quorum of home dc nodes. This might not be a bad model to follow for
				all consensus operations. But what if a datacenter becomes unavailable? That
				key will basically be dead until it can be reached again.

				problems:
					selecting the key owner will require a interdc consensus round. This would
					be ok for keys with regular r/w, but would be useless for keys that are
					used only once

			table replication: Have tables that are not replicated across datacenters.
 */

// wraps the conditional struct
// and handles all of the locking
type event struct {
	cond *sync.Cond
}

func newEvent() *event {
	return &event{cond: sync.NewCond(&sync.Mutex{})}
}

func (e *event) getChan() <- chan bool {
	c := make(chan bool)
	go func() {
		e.cond.L.Lock()
		e.cond.Wait()
		e.cond.L.Unlock()
		c <- true
	}()
	return c
}

func (e *event) wait() {
	<- e.getChan()
}

func (e *event) broadcast() {
	e.cond.Broadcast()
}

var consensusTimeoutEvent = func(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// returns a channel that will wake up after the given duration
func getTimeoutEvent(d time.Duration) <-chan time.Time {
	return consensusTimeoutEvent(d)
}

// replica level manager for consensus operations
type Manager struct {
	lock     sync.RWMutex
	cluster  cluster.Cluster
	stats    statsd.Statter

	instances    *InstanceMap
	inProgress   *InstanceMap
	committed    *InstanceMap

	executed     []InstanceID
	executedLock sync.RWMutex

	maxSeq       uint64
	maxSeqLock   sync.RWMutex
}

func NewManager(cluster cluster.Cluster) *Manager {
	stats, _ := statsd.NewNoop()
	return &Manager{
		cluster:  cluster,
		stats:    stats,
		instances:  NewInstanceMap(),
		inProgress: NewInstanceMap(),
		committed:  NewInstanceMap(),
		executed:   make([]InstanceID, 0, 16),
	}
}

func (m *Manager) setStatter(s statsd.Statter) {
	if s == nil {
		panic("cannot set nil statter")
	}
	m.stats = s
}

// returns the replicas for the manager's key, at the given  consistency level
func (m *Manager) getInstanceNodes(instance *Instance) []node.Node {
	return m.cluster.GetNodesForKey(instance.Command.Key)
}

func (m *Manager) checkLocalKeyEligibility(key string) bool {
	nodes := m.cluster.GetNodesForKey(key)
	localID := m.GetLocalID()
	for _, n := range nodes {
		if n.GetId() == localID {
			return true
		}
	}
	return false
}

func (m *Manager) checkLocalInstanceEligibility(instance *Instance) bool {
	return m.checkLocalKeyEligibility(instance.Command.Key)
}

// returns the replicas for the given instance's key, excluding the local node
func (m *Manager) getInstanceReplicas(instance *Instance) []node.Node {
	nodes := m.cluster.GetNodesForKey(instance.Command.Key)
	replicas := make([]node.Node, 0, len(nodes))
	for _, n := range nodes {
		if n.GetId() == m.GetLocalID() { continue }
		replicas = append(replicas, n)
	}
	return replicas
}

func (m *Manager) GetLocalID() node.NodeId {
	return m.cluster.GetID()
}

func (m *Manager) Query(instruction *store.Instruction) (store.Value, error) {

	if !m.checkLocalKeyEligibility(instruction.Key) {
		// need to iterate over the possible replicas, allowing for
		// some to be down
		panic("Forward to eligible replica not implemented yet")
	} else {
		val, err := m.ExecuteQuery(instruction)
		return val, err
	}

	return nil, nil
}

func (m *Manager) HandleMessage(msg message.Message) (message.Message, error) {
	switch request := msg.(type) {
	case *PreAcceptRequest:
		return m.HandlePreAccept(request)
	case *AcceptRequest:
		return m.HandleAccept(request)
	case *CommitRequest:
		return m.HandleCommit(request)
	case *PrepareRequest:
		return m.HandlePrepare(request)
	case *PrepareSuccessorRequest:
		return m.HandlePrepareSuccessor(request)
	default:
		return nil, fmt.Errorf("Unhandled request type: %T", request)
	}
	panic("unreachable")
}

// persists the manager's state to disk
func (m *Manager) Persist() error {
	m.statsInc("manager.persist", 1)
	return nil
}

func (m *Manager) statsInc(stat string, delta int64) error {
	return m.stats.Inc(stat, delta, STATS_SAMPLE_RATE)
}

func (m *Manager) statsGauge(stat string, delta int64) error {
	return m.stats.Gauge(stat, delta, STATS_SAMPLE_RATE)
}

func (m *Manager) statsTiming(stat string, start time.Time) error {
	end := time.Now()
	delta := end.Sub(start) / time.Millisecond
	return m.stats.Timing(stat, int64(delta), STATS_SAMPLE_RATE)
}

func (m *Manager) getInstance(iid InstanceID) *Instance {
	return m.instances.Get(iid)
}

// returns the local instance with the same id as the given instance,
// or sets the given instance locally. Does not handle any of the
// committed, inprogress, executed logic
func (m *Manager) getOrSetInstance(inst *Instance) (*Instance, bool) {
	initialize := func(i *Instance) {
		i.lock.Lock()
		defer i.lock.Unlock()
		i.manager = m
		if i.Status > INSTANCE_COMMITTED {
			i.Status = INSTANCE_COMMITTED
		}
	}
	return m.instances.GetOrSet(inst, initialize)
}

func (m *Manager) getInstructionDeps(instructions *store.Instruction) []InstanceID {
	numDeps := m.inProgress.Len() + m.committed.Len()
	deps := make([]InstanceID, 0, numDeps)
	for _, instance := range m.inProgress.Instances() {
		if m.cluster.CheckInterference(instructions, instance.Command) {
			deps = append(deps, instance.InstanceID)
		}
	}
	for _, instance := range m.committed.Instances() {
		if m.cluster.CheckInterference(instructions, instance.Command) {
			deps = append(deps, instance.InstanceID)
		}
	}
	return deps
}

func (m *Manager) getInstanceDeps(instance *Instance) []InstanceID {
	return m.getInstructionDeps(instance.Command)
}

// TODO: delete
// returns the next available sequence number for a new instance
// this method doesn't implement any locking or persistence
func (m *Manager) getNextSeqUnsafe() uint64 {
	return m.getNextSeq()
}

// returns the next available sequence number for a new instance
// this method doesn't implement any locking or persistence
func (m *Manager) getNextSeq() uint64 {
	m.maxSeqLock.Lock()
	defer m.maxSeqLock.Unlock()
	m.maxSeq++
	return m.maxSeq
}

// updates the manager's sequence number, if the given
// number is higher
func (m *Manager) updateSeq(seq uint64) error {
	existing := func() uint64 {
		m.maxSeqLock.RLock()
		defer m.maxSeqLock.RUnlock()
		return m.maxSeq
	}()

	if existing < seq {
		func() {
			m.maxSeqLock.Lock()
			defer m.maxSeqLock.Unlock()
			if m.maxSeq < seq {
				m.maxSeq = seq
			}
		}()
	}
	return nil
}

// creates a bare epaxos instance from the given instructions
func (m *Manager) makeInstance(instruction *store.Instruction) *Instance {
	instance := &Instance{
		InstanceID:   NewInstanceID(),
		LeaderID:     m.GetLocalID(),
		Command:      instruction,
		manager:	  m,
	}
	replicas := m.getInstanceReplicas(instance)
	instance.Successors = make([]node.NodeId, len(replicas))

	// add randomly ordered successors
	for i, j := range rand.Perm(len(replicas)) {
		instance.Successors[i] = replicas[j].GetId()
	}

	return instance
}

func (m *Manager) addMissingInstancesUnsafe(instances ...*Instance) error {
	for _, inst := range instances {
		if instance, existed := m.getOrSetInstance(inst); !existed {
			func(){
				instance.lock.Lock()
				defer instance.lock.Unlock()
				switch instance.Status {
				case INSTANCE_PREACCEPTED:
					m.statsInc("manager.missing_instance.preaccept", 1)
					logger.Debug("adding missing instance %v with status %v", instance.InstanceID, instance.Status)
					m.inProgress.Add(instance)
				case INSTANCE_ACCEPTED:
					m.statsInc("manager.missing_instance.accept", 1)
					logger.Debug("adding missing instance %v with status %v", instance.InstanceID, instance.Status)
					m.inProgress.Add(instance)
				case INSTANCE_COMMITTED, INSTANCE_EXECUTED:
					m.statsInc("manager.missing_instance.commit", 1)
					logger.Debug("adding missing instance %v with status %v", instance.InstanceID, instance.Status)
					instance.Status = INSTANCE_COMMITTED
					m.committed.Add(instance)
				default:
					panic("!")
				}
			}()
		}
	}
	return nil
}

func (m *Manager) addMissingInstances(instances ...*Instance) error {
	if err := m.addMissingInstancesUnsafe(instances...); err != nil {
		return err
	}
	if err := m.Persist(); err != nil {
		return err
	}
	return nil
}

func (m *Manager) updateInstanceBallotFromResponses(instance *Instance, responses []BallotMessage) error {
	var maxBallot uint32
	for _, response := range responses {
		if ballot := response.GetBallot(); ballot > maxBallot {
			maxBallot = ballot
		}
	}

	if instance.updateBallot(maxBallot) {
		if err := m.Persist(); err != nil {
			return err
		}
	}
	return nil
}

// executes a serialized query against the cluster this method designates the node
// it's called on as the command leader for the given query. Should only be called
// once per client query
func (m *Manager) ExecuteQuery(instruction *store.Instruction) (store.Value, error) {
	start := time.Now()
	defer m.statsTiming("manager.client.query.time", start)
	m.statsInc("manager.client.query.count", 1)

	// create epaxos instance, and preaccept locally
	instance := m.makeInstance(instruction)

	logger.Debug("Beginning preaccept leader phase for: %v", instance.InstanceID)
	// run pre-accept
	acceptRequired, err := m.preAcceptPhase(instance)
	if err != nil {
		return nil, err
	}

	logger.Debug("Preaccept leader phase completed for: %v", instance.InstanceID)

	if acceptRequired {
		logger.Debug("Beginning accept leader phase for: %v", instance.InstanceID)
		// some of the instance attributes received from the other replicas
		// were different from what was sent to them. Run the multi-paxos
		// accept phase
		if err := m.acceptPhase(instance); err != nil {
			return nil, err
		}
		logger.Debug("Accept leader phase completed for: %v", instance.InstanceID)
	}

	logger.Debug("Beginning commit leader phase for: %v", instance.InstanceID)
	// if we've gotten this far, either all the pre accept instance attributes
	// matched what was sent to them, or the correcting accept phase was successful
	// commit this instance
	if err := m.commitPhase(instance); err != nil {
		return nil, err
	}
	logger.Debug("Commit leader phase completed for: %v", instance.InstanceID)

	return m.executeInstance(instance)
}
