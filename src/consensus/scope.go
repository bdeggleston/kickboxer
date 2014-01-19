package consensus

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"
)

import (
	"github.com/looplab/tarjan"
)

import (
	"node"
	"store"
)

var (
	// timeout receiving a quorum of
	// preaccept responses
	PREACCEPT_TIMEOUT = uint64(500)

	// the amount of time a replica will wait
	// on a message with a preaccept status
	// before attempting to force a commit
	PREACCEPT_COMMIT_TIMEOUT = uint64(750)

	// timeout receiving a quorum of
	// accept responses
	ACCEPT_TIMEOUT = uint64(500)

	// the amount of time a replica will wait
	// on a message with a accept status
	// before attempting to force a commit
	ACCEPT_COMMIT_TIMEOUT = uint64(750)

	// timeout receiving a quorum of
	// prepare responses
	PREPARE_TIMEOUT = uint64(500)

	// the amount of time a replica will wait
	// on a message with a an attempted prepare
	// before attempting to force a commit again
	PREPARE_COMMIT_TIMEOUT = uint64(750)

	// the amount of time other goroutines will
	// wait for the local leader goroutine to
	// execute it's instance before it's assumed
	// to have failed, and they execute it
	EXECUTE_TIMEOUT = uint64(50)
)

/*
TODO: ->

1) explicit prepare
1a) Breakout preaccept, accept, and commit phases into methods (with stat counters)
	so they can be used by execute query and prepare methods

2) Workout a method for nullifying an instance
2a) remove instance rejected status

3) The scope needs to know the consistency level, and have a means of querying the cluster
	for the proper replicas each time it needs to send a message to the replicas. If a replica
	is added to, or removed from, the cluster mid transaction, the transaction will be executing
	against an out of date set of replicas.

4) long running integration tests. Each integration iteration should run on one core, with
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

5) scope state persistence. Maybe add system state mutation method to store?
	A triply nested hash table should do the trick for most applications.
		ex:
			consensus:
				<consensus_state> [scope_id][instance_id] = serialized_instance
			cluster state:
				<cluster_state> [node_id][node_property] = node_metadata

6) Workout a method for removing old nodes from the dependency graph

7) Add response expected param to execute instance, so queries that don't expect
	a return value can return even if it's instance dependencies have not been committed

8) Track metrics for:
		- number of rejected requests (Ballot)
		- number of explicit prepares (sent and received)
		- number of times / length of time waiting on dependencies to commit
		- quorum failures

9) Add a broadcast mechanism to notify pending executions that an instance has been committed

DONE:
	query execution

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

func makePreAcceptCommitTimeout() time.Time {
	return time.Now().Add(time.Duration(PREACCEPT_COMMIT_TIMEOUT) * time.Millisecond)
}

func makeAcceptCommitTimeout() time.Time {
	return time.Now().Add(time.Duration(ACCEPT_COMMIT_TIMEOUT) * time.Millisecond)
}

func makeExecuteTimeout() time.Time {
	return time.Now().Add(time.Duration(EXECUTE_TIMEOUT) * time.Millisecond)
}

func makeConditional() *sync.Cond {
	lock := &sync.Mutex{}
	lock.Lock()
	return sync.NewCond(lock)
}

type TimeoutError struct {
	message string
}

func (t TimeoutError) Error() string  { return t.message }
func (t TimeoutError) String() string { return t.message }
func NewTimeoutError(format string, a ...interface{}) TimeoutError {
	return TimeoutError{fmt.Sprintf(format, a...)}
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

	// wakes up goroutines waiting on instance commits
	commitNotify map[InstanceID]*sync.Cond

	// wakes up goroutines waiting on instance executions
	executeNotify map[InstanceID]*sync.Cond

	// ------------- runtime stats -------------

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
		commitNotify: make(map[InstanceID]*sync.Cond),
		executeNotify: make(map[InstanceID]*sync.Cond),
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

// returns the current dependencies for a new instance
// this method doesn't implement any locking or persistence
func (s *Scope) getCurrentDepsUnsafe() []InstanceID {
	// grab ALL instances as dependencies for now
	numDeps := len(s.inProgress) + len(s.committed)
	if len(s.executed) > 0 {
		numDeps += 1
	}

	deps := make([]InstanceID, 0, numDeps)
	deps = append(deps, s.inProgress.InstanceIDs()...)
	deps = append(deps, s.committed.InstanceIDs()...)

	if len(s.executed) > 0 {
		deps = append(deps, s.executed[len(s.executed)-1])
	}

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
	instance := &Instance{
		InstanceID:   NewInstanceID(),
		LeaderID:     s.GetLocalID(),
		Commands:     instructions,
	}

	return instance
}

func (s *Scope) addMissingInstancesUnsafe(instances ...*Instance) error {
	for _, instance := range instances {
		if !s.instances.ContainsID(instance.InstanceID) {
			switch instance.Status {
			case INSTANCE_PREACCEPTED:
				instance.commitTimeout = makePreAcceptCommitTimeout()
				s.inProgress.Add(instance)
			case INSTANCE_ACCEPTED:
				instance.commitTimeout = makeAcceptCommitTimeout()
				s.inProgress.Add(instance)
			case INSTANCE_REJECTED:
				panic("rejected instances not handled yet")
			case INSTANCE_COMMITTED:
				s.committed.Add(instance)
			case INSTANCE_EXECUTED:
				instance.Status = INSTANCE_COMMITTED
				s.committed.Add(instance)
			}
			s.instances.Add(instance)
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

// sorts the strongly connected subgraph components
type iidSorter struct {
	scope *Scope
	iids []InstanceID
}

func (i *iidSorter) Len() int {
	return len(i.iids)
}

// returns true if the item at index x is less than
// the item and index y
func (i *iidSorter) Less(x, y int) bool {
	i0 := i.scope.instances[i.iids[x]]
	i1 := i.scope.instances[i.iids[y]]

	// first check the sequence#
	if i0.Sequence != i1.Sequence {
		return i0.Sequence < i1.Sequence
	} else {
		// then the embedded timestamp
		t0, _ := i0.InstanceID.UUID().Time()
		t1, _ := i1.InstanceID.UUID().Time()
		if t0 != t1 {
			return t0 < t1
		} else {
			// finally the lexicographic comparison
			return bytes.Compare([]byte(i0.InstanceID), []byte(i1.InstanceID)) == -1
		}
	}
	return false
}

// switches the position of nodes at indices i & j
func (i *iidSorter) Swap(x, y int) {
	i.iids[x], i.iids[y] = i.iids[y], i.iids[x]
}

// topologically sorts instance dependencies, grouped by strongly
// connected components
func (s *Scope) getExecutionOrder(instance *Instance) []InstanceID {
	s.lock.RLock()
	defer s.lock.RUnlock()

	// build a directed graph
	depGraph := make(map[interface {}][]interface {}, len(instance.Dependencies) + 1)
	addInstance := func(inst *Instance) {
		iids := make([]interface {}, len(inst.Dependencies))
		for i, iid := range inst.Dependencies {
			iids[i] = iid
		}
		depGraph[inst.InstanceID] = iids
	}
	addInstance(instance)
	for _, iid := range instance.Dependencies {
		inst := s.instances[iid]
		if inst == nil { panic(fmt.Sprintf("Unknown instance id: %v", iid)) }
		addInstance(inst)
	}

	// sort with tarjan's algorithm
	tSorted := tarjan.Connections(depGraph)
	exOrder := make([]InstanceID, 0, len(instance.Dependencies) + 1)
	for _, set := range tSorted {
		iids := make([]InstanceID, len(set))
		for i, iid := range set {
			iids[i] = iid.(InstanceID)
		}
		sorter := &iidSorter{scope:s, iids:iids}
		sort.Sort(sorter)
		exOrder = append(exOrder, sorter.iids...)
	}

	return exOrder
}

func (s *Scope) getUncommittedInstances(iids []InstanceID) []*Instance {
	s.lock.RLock()
	defer s.lock.RUnlock()
	instances := make([]*Instance, 0)
	for _, iid := range iids {
		instance := s.instances[iid]
		if instance.Status < INSTANCE_COMMITTED {
			instances = append(instances, instance)
		}
	}

	return instances
}

// executes and instance against the store
func (s *Scope) applyInstance(instance *Instance) (store.Value, error) {
	if instance.Status != INSTANCE_COMMITTED {
		return nil, fmt.Errorf("instance not committed")
	}
	var val store.Value
	var err error
	for _, instruction := range instance.Commands {
		val, err = s.manager.cluster.ApplyQuery(
			instruction.Cmd,
			instruction.Key,
			instruction.Args,
			instruction.Timestamp,
		)
		if err != nil {
			return nil, err
		}
	}

	// update scope bookkeeping
	instance.Status = INSTANCE_EXECUTED
	delete(s.committed, instance.InstanceID)
	s.executed = append(s.executed, instance.InstanceID)
	if err := s.Persist(); err != nil {
		return nil, err
	}
	s.statExecuteCount++

	// wake up any goroutines waiting on this instance,
	// and remove the conditional from the notify map
	if cond, ok := s.executeNotify[instance.InstanceID]; ok {
		cond.Broadcast()
		delete(s.executeNotify, instance.InstanceID)
	}

	return val, nil
}
// executes the dependencies up to the given instance
func (s *Scope) executeDependencyChain(iids []InstanceID, target *Instance) (store.Value, error) {
	// TODO: don't execute instances 'out from under' client requests. Use an execution grace period
	// first, check the leader id, if it's not this node, go ahead and execute it
	// if it is, wait for the execution timeout
	var val store.Value
	var err error

	// applies the instance and unlocks the lock, even if the apply
	// call panics. The lock must be aquired before calling this
	applyAndUnlock := func(instance *Instance) (store.Value, error) {
		defer s.lock.Unlock()
		return s.applyInstance(instance)
	}

	for _, iid := range iids {
		val = nil
		err = nil
		instance := s.getInstance(iid)
		s.lock.Lock()
		switch instance.Status {
		case INSTANCE_COMMITTED:
			//
			if instance.InstanceID == target.InstanceID {
				// execute
				val, err = applyAndUnlock(instance)
				if err != nil { return nil, err }
				s.statExecuteLocalSuccess++
			} else if instance.LeaderID != s.manager.GetLocalID() {
				// execute
				val, err = applyAndUnlock(instance)
				if err != nil { return nil, err }
				s.statExecuteRemote++
			} else {
				// wait for the execution grace period to end
				if time.Now().After(instance.executeTimeout) {
					val, err = applyAndUnlock(instance)
					if err != nil { return nil, err }
					s.statExecuteLocalTimeout++
				} else {
					// get or create broadcast object
					cond, ok := s.executeNotify[instance.InstanceID]
					if !ok {
						cond = makeConditional()
						s.executeNotify[instance.InstanceID] = cond
					}

					// wait on broadcast event or timeout
					broadcastEvent := make(chan bool)
					go func() {
						cond.Wait()
						broadcastEvent <- true
					}()
					timeoutEvent := time.After(instance.executeTimeout.Sub(time.Now()))
					s.lock.Unlock()

					select {
					case <- broadcastEvent:
						// instance was executed by another goroutine
						s.statExecuteLocalSuccessWait++
					case <- timeoutEvent:
						// execution timed out
						s.lock.Lock()

						// check that instance was not executed by another
						// waking goroutine
						if instance.Status != INSTANCE_COMMITTED {
							// unlock and continue if it was
							s.lock.Unlock()
						} else {
							val, err = applyAndUnlock(instance)
							if err != nil { return nil, err }
							s.statExecuteLocalTimeout++
							s.statExecuteLocalTimeoutWait++
						}
					}
				}
			}
		case INSTANCE_EXECUTED, INSTANCE_REJECTED:
			s.lock.Unlock()
			continue
		default:
			return nil, fmt.Errorf("Uncommitted dependencies should be handled before executeDependencyChain")
		}

		// only execute up to the target instance
		if instance.InstanceID == target.InstanceID {
			break
		}
	}

	return val, nil
}

// applies an instance to the store
// first it will resolve all dependencies, then wait for them to commit/reject, or force them
// to do one or the other. Then it will execute it's committed dependencies, then execute itself
func (s *Scope) executeInstance(instance *Instance) (store.Value, error) {
	// get dependency instance ids, sorted in execution order
	exOrder := s.getExecutionOrder(instance)

	uncommitted := s.getUncommittedInstances(exOrder)
	if len(uncommitted) > 0 {
		panic("explicit prepare not implemented yet")
	}

	return s.executeDependencyChain(exOrder, instance)
}

// sends and explicit prepare request to the other nodes. Returns a channel for receiving responses on
// This method does not wait for the responses to return, because the scope needs to be locked during
// message sending, so the instance is not updated, but does not need to be locked while receiving responses
func (s *Scope) sendPrepare(instance *Instance, replicas []node.Node) (<- chan *PrepareResponse, error) {
	recvChan := make(chan *PrepareResponse, len(replicas))
	msg := &PrepareRequest{Scope:s.name, Ballot:instance.MaxBallot, InstanceID:instance.InstanceID}
	sendMsg := func(n node.Node) {
		if response, err := n.SendMessage(msg); err != nil {
			logger.Warning("Error receiving PrepareResponse: %v", err)
		} else {
			if preAccept, ok := response.(*PrepareResponse); ok {
				recvChan <- preAccept
			} else {
				logger.Warning("Unexpected Prepare response type: %T", response)
			}
		}
	}

	for _, replica := range replicas {
		go sendMsg(replica)
	}

	return recvChan, nil
}

func (s *Scope) receivePrepareResponseQuorum(recvChan <-chan *PrepareResponse, instance *Instance, quorumSize int, numReplicas int) ([]*PrepareResponse, error) {
	numReceived := 1  // this node counts as a response
	timeoutEvent := time.After(time.Duration(PREPARE_TIMEOUT) * time.Millisecond)
	var response *PrepareResponse
	responses := make([]*PrepareResponse, 0, numReplicas)
	for numReceived < quorumSize {
		select {
		case response = <-recvChan:
			logger.Debug("Prepare response received: %v", instance.InstanceID)
			responses = append(responses, response)
			numReceived++
		case <-timeoutEvent:
			logger.Debug("Prepare timeout for instance: %v", instance.InstanceID)
			return nil, NewTimeoutError("Timeout while awaiting pre accept responses")
		}
	}

	// finally, receive any additional responses
	drain: for {
		select {
		case response = <-recvChan:
			responses = append(responses, response)
			numReceived++
		default:
			break drain
		}
	}

	return responses, nil
}

// runs explicit prepare phase on instances where a command leader failure is suspected
// during execution,
// TODO:
//	what happens if 2 nodes send each other prepare messages at the same time?
func (s *Scope) prepareInstance(instance *Instance) error {
	replicas := s.manager.getScopeReplicas(s)

	// increments and sends the prepare messages in a single lock
	incrementInstanceAndSendPrepareMessage := func() (<-chan *PrepareResponse, error) {
		s.lock.Lock()
		defer s.lock.Unlock()
		instance.MaxBallot++
		if err := s.Persist(); err != nil {
			return nil, err
		}
		return s.sendPrepare(instance, replicas)
	}
	recvChan, err := incrementInstanceAndSendPrepareMessage()
	if err != nil { return err }

	// receive responses from at least a quorum of nodes
	quorumSize := ((len(replicas) + 1) / 2) + 1
	responses, err := s.receivePrepareResponseQuorum(recvChan, instance, quorumSize, len(replicas))
	if err != nil { return err }

	// find the highest response ballot
	maxBallot := uint32(0)
	for _, response := range responses {
		if ballot := response.Instance.MaxBallot; ballot > maxBallot {
			maxBallot = ballot
		}
	}

	// find the highest response status
	maxStatus := InstanceStatus(byte(0))
	for _, response := range responses {
		if status := response.Instance.Status; status > maxStatus {
			maxStatus = status
		}
	}

	switch maxStatus {
	case INSTANCE_PREACCEPTED:
		// run pre accept phase
		fallthrough
	case INSTANCE_ACCEPTED:
		// run accept phase
		fallthrough
	case INSTANCE_COMMITTED, INSTANCE_EXECUTED:
		// commit instance


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

	// run pre-accept
	acceptRequired, err := s.preAcceptPhase(instance)
	if err != nil {
		return nil, err
	}

	if acceptRequired {
		// some of the instance attributes received from the other replicas
		// were different from what was sent to them. Run the multi-paxos
		// accept phase
		if err := s.acceptPhase(instance); err != nil {
			return nil, err
		}
	}

	// if we've gotten this far, either all the pre accept instance attributes
	// matched what was sent to them, or the correcting accept phase was successful
	// commit this instance
	if err := s.commitPhase(instance); err != nil {
		return nil, err
	}

	return s.executeInstance(instance)
}

