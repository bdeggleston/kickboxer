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
	"message"
	"node"
	"store"
	"topology"
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

	// flag enabling additional instance info to be kept
	PAXOS_DEBUG = false
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

TODO: remove commit timeouts, replace with last activity (last message sent received)
TODO: add missing instances "requests" to preaccept, accept, and commit message replies

Prepare successors should increment the last activity time outs by their order in the successor list. The
farther they are from first successor, the higher their timeout should be

TODO: make the prepare phase a bit smarter

Prepare phase improvement:
If the prepare responses are all preaccepted instances with the dependency match flag set, prepare should send a commit
If the prepare responses are all preaccepted instances with the same ballot, the dependencies should be merged and an
	accept message should be sent.
If the prepare responses are all accepted instances with the same ballot, seq, and deps, a commit message should be sent
If any response has a committed or executed messages, a commit message should be sent to all replicas

TODO: work out a way to prioritize the completion of existing commands vs the processing of new ones

Keep a count of instances in progress, and delay new queries proportionally

TODO: think about potential race conditions caused by multiple goroutines running executeDependencyChain concurrently

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
	nodeID   node.NodeId
	dcID     topology.DatacenterID
	store	 store.Store
	topology *topology.Topology
	stats    statsd.Statter

	instances    *InstanceMap

	executed     []InstanceID
	executedLock sync.RWMutex

	depsMngr  *dependencyManager
}

func NewManager(
		t *topology.Topology,
		s store.Store,
	) *Manager {
	stats, _ := statsd.NewNoop()
	mngr := &Manager{
		nodeID: t.GetLocalNodeID(),
		dcID: t.GetLocalDatacenterID(),
		store: s,
		topology: t,
		stats:    stats,
		instances:  NewInstanceMap(),
		executed:   make([]InstanceID, 0, 16),
	}

	mngr.depsMngr = newDependencyManager(mngr)
	return mngr
}

func (m *Manager) setStatter(s statsd.Statter) {
	if s == nil {
		panic("cannot set nil statter")
	}
	m.stats = s
}

// returns the replicas for the manager's key, at the given  consistency level
func (m *Manager) getInstanceNodes(instance *Instance) []topology.Node {
	tk := m.topology.GetToken(instance.Command.Key)
	return m.topology.GetLocalNodesForToken(tk)
}

func (m *Manager) checkLocalKeyEligibility(key string) bool {
	tk := m.topology.GetToken(key)
	return m.topology.TokenLocallyReplicated(tk)
}

func (m *Manager) checkLocalInstanceEligibility(instance *Instance) bool {
	return m.checkLocalKeyEligibility(instance.Command.Key)
}

// returns the replicas for the given instance's key, excluding the local node
func (m *Manager) getInstanceReplicas(instance *Instance) []node.Node {
	tk := m.topology.GetToken(instance.Command.Key)
	nodes := m.topology.GetLocalNodesForToken(tk)
	replicas := make([]node.Node, 0, len(nodes))
	for _, n := range nodes {
		if n.GetId() == m.nodeID { continue }
		replicas = append(replicas, n)
	}
	return replicas
}

func (m *Manager) GetLocalID() node.NodeId {
	return m.topology.GetLocalNodeID()
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
	instance, existed := m.instances.GetOrSet(inst, initialize)
	if !existed && instance.Status > INSTANCE_PREACCEPTED {
		m.depsMngr.AddDependency(instance)
	}
	return instance, existed
}

var managerGetInstanceDeps = func(m *Manager, instance *Instance) ([]InstanceID, error) {
	return m.depsMngr.GetAndSetDeps(instance)
}

func (m *Manager) getInstanceDeps(instance *Instance) ([]InstanceID, error) {
	return managerGetInstanceDeps(m, instance)
}

// creates a bare epaxos instance from the given instructions
func (m *Manager) makeInstance(instruction store.Instruction) *Instance {
	instance := &Instance{
		InstanceID:   NewInstanceID(),
		LeaderID:     m.nodeID,
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
			err := func() error {
				instance.lock.Lock()
				defer instance.lock.Unlock()
				switch instance.Status {
				case INSTANCE_PREACCEPTED:
					m.statsInc("manager.missing_instance.preaccept", 1)
					logger.Debug("adding missing instance %v with status %v", instance.InstanceID, instance.Status)
				case INSTANCE_ACCEPTED:
					m.statsInc("manager.missing_instance.accept", 1)
					logger.Debug("adding missing instance %v with status %v", instance.InstanceID, instance.Status)
				case INSTANCE_COMMITTED, INSTANCE_EXECUTED:
					m.statsInc("manager.missing_instance.commit", 1)
					logger.Debug("adding missing instance %v with status %v", instance.InstanceID, instance.Status)
					instance.Status = INSTANCE_COMMITTED
				default:
					panic(fmt.Errorf("Unknown status: %v", instance.Status))
				}

				if err := m.depsMngr.AddDependency(instance); err != nil {
					return err
				}
				return nil
			}()
			if err != nil {
				return err
			}
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
func (m *Manager) ExecutePaxos(instance *Instance) {
	start := time.Now()
	defer m.statsTiming("manager.client.query.time", start)
	m.statsInc("manager.client.query.count", 1)

	logger.Debug("Beginning preaccept leader phase for: %v", instance.InstanceID)
	// run pre-accept
	acceptRequired, err := m.preAcceptPhase(instance)
	if err != nil {
		logger.Info("Error running preaccept phase instance: %v", err)
	}

	logger.Debug("Preaccept leader phase completed for: %v", instance.InstanceID)

	if acceptRequired {
		logger.Debug("Beginning accept leader phase for: %v", instance.InstanceID)
		// some of the instance attributes received from the other replicas
		// were different from what was sent to them. Run the multi-paxos
		// accept phase
		if err := m.acceptPhase(instance); err != nil {
			logger.Info("Error running accept phase instance: %v", err)
		}
		logger.Debug("Accept leader phase completed for: %v", instance.InstanceID)
	}

	logger.Debug("Beginning commit leader phase for: %v", instance.InstanceID)
	// if we've gotten this far, either all the pre accept instance attributes
	// matched what was sent to them, or the correcting accept phase was successful
	// commit this instance
	if err := m.commitPhase(instance); err != nil {
		logger.Info("Error running commit phase instance: %v", err)
	}
	logger.Debug("Commit leader phase completed for: %v", instance.InstanceID)

	m.executeInstance(instance)
}

func (m *Manager) ExecuteQuery(instruction store.Instruction) (store.Value, error) {

	if !m.checkLocalKeyEligibility(instruction.Key) {
		// need to iterate over the possible replicas, allowing for
		// some to be down
		panic("Forward to eligible replica not implemented yet")
	}

	// create epaxos instance, and preaccept locally
	instance := m.makeInstance(instruction)
	resultListener := instance.addListener()

	go m.ExecutePaxos(instance)

	var result InstanceResult
	select {
	case result = <-resultListener:
		return result.val, result.err
	}

	return nil, nil
}

