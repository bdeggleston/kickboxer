package consensus

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

import (
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
	SUCCESSOR_TIMEOUT = uint64(500)

	// how often a node should check that the
	// successor is up while waiting on a prepare phase
	SUCCESSOR_CONTACT_INTERVAL = uint64(500)

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
)

/*

TODO: implement an order of prepare phase succession, enable other replicas to initiate it

Prepare succession:
At instance creation time, a list of the other replica node ids is added to the instance, randomly ordered.
After a commit timeout, the node waiting on the commit should send a message asking the node to return the
result of a prepare phase. If the node doesn't respond in time, it continues down the successor list until
it receives a response, or it's next in line.
If a commit timeout occurs and the node it occurs in is the first in line, it will run the prepare phase.
When non leader node calculates a commit timeout, it should use it's position in the successor list as
a variable in computing it.

When should a prepare successor request be sent, and what should be done when one is received?
	Should it just be like a heartbeat route that returns a copy of the requested instance?

	It should check if the instance has been committed, and initiate a prepare phase if it hasn't. The prepare
		phase can keep track of when and if to prepare. As long as there's a prepare mutex on an instance,
		this should be ok.

	If the instance is not known to the successor, it should return a nil instance, indicating the next
		successor should be tried

	If a non successor node was not able to communicate with the first node, but received a response from
		the second, should it talk to the first, or second node when is goes through another successor
		communication phase, or should it start with the first one?

	If a non successor node is not able to communicate with successor[0], but is able to communicate with
		successor[1] BUT successor[1] IS able to communicate with successor[0], it's ok, because the
		prepare phase call on successorp1[ will defer to successor[0]

TODO: remove commit timeouts, replace with last activity (last message sent received)

TODO: make the prepare phase a bit smarter

Prepare phase improvement:
If the prepare responses are all preaccepted instances with the dependency match flag set, prepare should send an accept
If the prepare responses are all preaccepted instances with the same ballot, the dependencies should be merged and an
	accept message should be sent.
If the prepare responses are all accepted instances with the same ballot, seq, and deps, a commit message should be sent
If any response has a committed or executed messages, a commit message should be sent to all replicas

TODO: work out a way to prioritize the completion of existing commands vs the processing of new ones

TODO: implement instance level locking

Instance level locking would allow multiple instances to be serviced concurrently, even if they belong to
the same scope. Would need to add a method to get or set instances on scope, get instances, and a method
of updating both instance attributes and scope bookkeeping while both are locked. May need to move some
functionality onto the instance, and make all instances aware of the local scope (could be handled in the
get or set method)

1) The scope needs to know the consistency level, and have a means of querying the cluster
	for the proper replicas each time it needs to send a message to the replicas. If a replica
	is added to, or removed from, the cluster mid transaction, the transaction will be executing
	against an out of date set of replicas.

2) scope state persistence. Maybe add system state mutation method to store?
	A triply nested hash table should do the trick for most applications.
		ex:
			consensus:
				<consensus_state> [scope_id][instance_id] = serialized_instance
			cluster state:
				<cluster_state> [node_id][node_property] = node_metadata

3) Workout a method for removing old nodes from the dependency graph

4) Add response expected param to execute instance, so queries that don't expect
	a return value can return even if it's instance dependencies have not been committed

5) Track metrics for:
		- number of rejected requests (Ballot)
		- number of explicit prepares (sent and received)
		- number of times / length of time waiting on dependencies to commit
		- quorum failures

6) Add a broadcast mechanism to notify pending executions that an instance has been committed

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
	key's consensus scope. That should allow it to start participating in the consensus
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
				home dc could be moved automatically based on query frequency. Reassigning a scope's
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

func makeConditional() *sync.Cond {
	lock := &sync.Mutex{}
	return sync.NewCond(lock)
}

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

// manages a subset of interdependent
// consensus operations
type Scope struct {
	name         string
	instances    InstanceMap
	inProgress   InstanceMap
	committed    InstanceMap
	executed     []InstanceID
	maxSeq       uint64
	lock         sync.RWMutex
	cmdLock      sync.Mutex
	manager      *Manager
	persistCount uint64

	// prevents multiple goroutines from attempting
	// an explicit prepare on the same instance
	prepareLock map[InstanceID]*sync.Mutex

	// ------------- runtime stats -------------

	// ------------- commit stats -------------

	// total number of committed instances
	statCommitCount uint64

	// total number of timed out commits
	statCommitTimeout uint64

	// number of times a goroutine was waiting on
	// an instance to execute
	statCommitTimeoutWait uint64

	// ------------- execution stats -------------

	// total number of executed instances
	statExecuteCount uint64

	// number of times a local instance was not executed
	// by it's originating goroutine because it's execution
	// grace period had passed
	statExecuteLocalTimeout uint64

	// number of times a local instance was not executed
	// by it's originating goroutine because it timed out
	// while another goroutine was waiting on it
	statExecuteLocalTimeoutWait uint64

	// number of times a local instances was executed
	// by it's originating goroutine
	statExecuteLocalSuccess uint64

	// number of times a goroutine was waiting on a local
	// instance to execute
	statExecuteLocalSuccessWait uint64

	// number of remote instances executed
	statExecuteRemote uint64
}

func NewScope(name string, manager *Manager) *Scope {
	return &Scope{
		name:       name,
		instances:  NewInstanceMap(),
		inProgress: NewInstanceMap(),
		committed:  NewInstanceMap(),
		executed:   make([]InstanceID, 0, 16),
		manager:    manager,
		prepareLock: make(map[InstanceID]*sync.Mutex),
	}
}

func (s *Scope) GetLocalID() node.NodeId {
	return s.manager.GetLocalID()
}

// persists the scope's state to disk
func (s *Scope) Persist() error {
	s.persistCount++
	return nil
}

func (s *Scope) getInstance(iid InstanceID) *Instance {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.instances[iid]
}

func (s *Scope) setInstanceStatus(instance *Instance, status InstanceStatus) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	instance.Status = status
	if err := s.Persist(); err != nil {
		return err
	}
	return nil
}

func (s *Scope) copyInstanceUnsafe(src *Instance) (*Instance, error) {
	buf := &bytes.Buffer{}
	writer := bufio.NewWriter(buf)
	if err := instanceSerialize(src, writer); err != nil {
		return nil, err
	}
	if err := writer.Flush(); err != nil {
		return nil, err
	}
	reader := bufio.NewReader(buf)
	dst, err := instanceDeserialize(reader)
	if err != nil {
		return nil, err
	}
	return dst, nil
}

// copies an instance in the contect of a lock
func (s *Scope) copyInstanceAtomic(src *Instance) (*Instance, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.copyInstanceUnsafe(src)
}

func (s *Scope) debugInstanceLog(instance *Instance, format string, args ...interface {}) {
	message := fmt.Sprintf(format, args...)
	logger.Debug("%v for instance %v on node: %v", message, instance.InstanceID, s.GetLocalID())
}

// returns the current dependencies for a new instance
// this method doesn't implement any locking or persistence
func (s *Scope) getCurrentDepsUnsafe() []InstanceID {
	// grab ALL instances as dependencies for now
	numDeps := len(s.inProgress) + len(s.committed) + len(s.executed)
//	if len(s.executed) > 0 {
//		numDeps += 1
//	}

	deps := make([]InstanceID, 0, numDeps)
	deps = append(deps, s.inProgress.InstanceIDs()...)
	deps = append(deps, s.committed.InstanceIDs()...)
	deps = append(deps, s.executed...)
//	if len(s.executed) > 0 {
//		deps = append(deps, s.executed[len(s.executed)-1])
//	}

	return deps
}

// returns the next available sequence number for a new instance
// this method doesn't implement any locking or persistence
func (s *Scope) getNextSeqUnsafe() uint64 {
	s.maxSeq++
	return s.maxSeq
}

// creates a bare epaxos instance from the given instructions
func (s *Scope) makeInstance(instructions []*store.Instruction) *Instance {
	replicas := s.manager.getScopeReplicas(s)
	instance := &Instance{
		InstanceID:   NewInstanceID(),
		LeaderID:     s.GetLocalID(),
		Commands:     instructions,
		Successors:   make([]node.NodeId, len(replicas)),
	}

	// add randomly ordered successors
	for i, j := range rand.Perm(len(replicas)) {
		instance.Successors[i] = replicas[j].GetId()
	}

	return instance
}

func (s *Scope) addMissingInstancesUnsafe(instances ...*Instance) error {
	for _, instance := range instances {
		if !s.instances.ContainsID(instance.InstanceID) {
			switch instance.Status {
			case INSTANCE_PREACCEPTED:
				logger.Debug("adding missing instance %v with status %v", instance.InstanceID, instance.Status)
				if err := s.preAcceptInstanceUnsafe(instance, false); err != nil {
					return nil
				}
			case INSTANCE_ACCEPTED:
				logger.Debug("adding missing instance %v with status %v", instance.InstanceID, instance.Status)
				if err := s.acceptInstanceUnsafe(instance, false); err != nil {
					return err
				}
			case INSTANCE_COMMITTED, INSTANCE_EXECUTED:
				logger.Debug("adding missing instance %v with status %v", instance.InstanceID, instance.Status)
				if err := s.commitInstanceUnsafe(instance, false); err != nil {
					return err
				}
			default:
				panic("!")
			}
		}
	}
	return nil
}

func (s *Scope) addMissingInstances(instances ...*Instance) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := s.addMissingInstancesUnsafe(instances...); err != nil {
		return err
	}
	if err := s.Persist(); err != nil {
		return err
	}
	return nil
}

func (s *Scope) updateInstanceBallotFromResponses(instance *Instance, responses []BallotMessage) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	ballot := instance.MaxBallot
	for _, response := range responses {
		if response.GetBallot() > ballot {
			ballot = response.GetBallot()
		}
	}

	if ballot > instance.MaxBallot {
		instance.MaxBallot = ballot
	}
	if err := s.Persist(); err != nil {
		return err
	}
	return nil
}

// executes a serialized query against the cluster
// this method designates the node it's called on as the command leader for the given query
// and therefore, should only be called once per client query
func (s *Scope) ExecuteQuery(instructions []*store.Instruction) (store.Value, error) {

	if !s.manager.checkLocalScopeEligibility(s) {
		return nil, fmt.Errorf("This node is not eligible to act as the command leader for this scope")
	}

	// create epaxos instance, and preaccept locally
	instance := s.makeInstance(instructions)

	logger.Debug("Beginning preaccept leader phase for: %v", instance.InstanceID)
	// run pre-accept
	acceptRequired, err := s.preAcceptPhase(instance)
	if err != nil {
		return nil, err
	}

	logger.Debug("Preaccept leader phase completed for: %v", instance.InstanceID)

	if acceptRequired {
		logger.Debug("Beginning accept leader phase for: %v", instance.InstanceID)
		// some of the instance attributes received from the other replicas
		// were different from what was sent to them. Run the multi-paxos
		// accept phase
		if err := s.acceptPhase(instance); err != nil {
			return nil, err
		}
		logger.Debug("Accept leader phase completed for: %v", instance.InstanceID)
	}

	logger.Debug("Beginning commit leader phase for: %v", instance.InstanceID)
	// if we've gotten this far, either all the pre accept instance attributes
	// matched what was sent to them, or the correcting accept phase was successful
	// commit this instance
	if err := s.commitPhase(instance); err != nil {
		return nil, err
	}
	logger.Debug("Commit leader phase completed for: %v", instance.InstanceID)

	return s.executeInstance(instance)
}

