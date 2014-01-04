package consensus

import (
	"fmt"
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
)

/*
TODO: ->


1) The scope needs to know the consistency level, and have a means of querying the cluster
	for the proper replicas each time it needs to send a message to the replicas. If a replica
	is added to, or removed from, the cluster mid transaction, the transaction will be executing
	against an out of date set of replicas.

2) query execution

3) query repair

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
 */

func makePreAcceptCommitTimeout() time.Time {
	return time.Now().Add(time.Duration(PREACCEPT_COMMIT_TIMEOUT) * time.Millisecond)
}

func makeAcceptCommitTimeout() time.Time {
	return time.Now().Add(time.Duration(ACCEPT_COMMIT_TIMEOUT) * time.Millisecond)
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
}

func NewScope(name string, manager *Manager) *Scope {
	return &Scope{
		name:       name,
		instances:  NewInstanceMap(),
		inProgress: NewInstanceMap(),
		committed:  NewInstanceMap(),
		executed:   make([]InstanceID, 0, 16),
		manager:    manager,
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

func (s *Scope) preAcceptInstanceUnsafe(instance *Instance) (bool, error) {
	if existing, exists := s.instances[instance.InstanceID]; exists {
		if existing.Status >= INSTANCE_PREACCEPTED {
			return false, nil
		}
	} else {
		s.instances.Add(instance)
	}

	instance.Status = INSTANCE_PREACCEPTED
	instance.Dependencies = s.getCurrentDepsUnsafe()
	instance.Sequence = s.getNextSeqUnsafe()
	instance.commitTimeout = makePreAcceptCommitTimeout()
	s.inProgress.Add(instance)

	if err := s.Persist(); err != nil {
		return false, err
	}

	return true, nil
}

// sets the given instance to preaccepted and updates, deps,
// seq, and commit timeout
// in the case of handling messages from leaders to replicas
// the message instance should be passed in. It will either
// update the existing instance in place, or add the message
// instance to the scope's instance
// returns a bool indicating that the instance was actually
// accepted (and not skipped), and an error, if applicable
func (s *Scope) preAcceptInstance(instance *Instance) (bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.preAcceptInstanceUnsafe(instance)
}

// sends pre accept responses to the given replicas, and returns their responses. An error will be returned
// if there are problems, or a quorum of responses were not received within the timeout
func (s *Scope) sendPreAccept(instance *Instance, replicas []node.Node) ([]*PreAcceptResponse, error) {
	//
	recvChan := make(chan *PreAcceptResponse, len(replicas))
	msg := &PreAcceptRequest{Scope: s.name, Instance: instance}
	sendMsg := func(n node.Node) {
		if response, err := n.SendMessage(msg); err != nil {
			logger.Warning("Error receiving PreAcceptResponse: %v", err)
		} else {
			if preAccept, ok := response.(*PreAcceptResponse); ok {
				recvChan <- preAccept
			} else {
				logger.Warning("Unexpected PreAccept response type: %T", response)
			}
		}
	}
	for _, replica := range replicas {
		go sendMsg(replica)
	}

	numReceived := 1 // this node counts as a response
	quorumSize := ((len(replicas) + 1) / 2) + 1
	timeoutEvent := time.After(time.Duration(PREACCEPT_TIMEOUT) * time.Millisecond)
	var response *PreAcceptResponse
	responses := make([]*PreAcceptResponse, 0, len(replicas))
	for numReceived < quorumSize {
		select {
		case response = <-recvChan:
			logger.Debug("PreAccept response received: %v", instance.InstanceID)
			responses = append(responses, response)
			numReceived++
		case <-timeoutEvent:
			logger.Debug("PreAccept timeout for instance: %v", instance.InstanceID)
			return nil, NewTimeoutError("Timeout while awaiting pre accept responses")
		}
	}

	// check if any of the messages were rejected
	accepted := true
	for _, response := range responses {
		accepted = accepted && response.Accepted
	}

	// handle rejected pre-accept messages
	if !accepted {
		// update max ballot from responses
		bmResponses := make([]BallotMessage, len(responses))
		for i, response := range responses {
			bmResponses[i] = BallotMessage(response)
		}
		s.updateInstanceBallotFromResponses(instance, bmResponses)
		// TODO: what to do here... Try again?
		panic("rejected pre-accept not handled yet")
	}

	return responses, nil
}

// merges the attributes from the pre accept responses onto the local instance
// and returns a bool indicating if any changes were made
func (s *Scope) mergePreAcceptAttributes(instance *Instance, responses []*PreAcceptResponse) (bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	changes := false
	for _, response := range responses {
		changes = changes || instance.mergeAttributes(response.Instance.Sequence, response.Instance.Dependencies)
	}
	if err := s.Persist(); err != nil {
		return true, err
	}
	// TODO: handle MissingInstances included in the responses
	return changes, nil
}

// sets the given instance as accepted
// in the case of handling messages from leaders to replicas
// the message instance should be passed in. It will either
// update the existing instance in place, or add the message
// instance to the scope's instance
// returns a bool indicating that the instance was actually
// accepted (and not skipped), and an error, if applicable
func (s *Scope) acceptInstanceUnsafe(instance *Instance) (bool, error) {
	if existing, exists := s.instances[instance.InstanceID]; exists {
		if existing.Status >= INSTANCE_ACCEPTED {
			return false, nil
		} else {
			existing.Status = INSTANCE_ACCEPTED
			existing.Dependencies = instance.Dependencies
			existing.Sequence = instance.Sequence
			existing.MaxBallot = instance.MaxBallot
		}
	} else {
		s.instances.Add(instance)
	}

	instance.Status = INSTANCE_ACCEPTED
	instance.commitTimeout = makeAcceptCommitTimeout()
	s.inProgress.Add(instance)

	if instance.Sequence > s.maxSeq {
		s.maxSeq = instance.Sequence
	}

	if err := s.Persist(); err != nil {
		return false, err
	}
	return true, nil
}

// sets the given instance as accepted
// in the case of handling messages from leaders to replicas
// the message instance should be passed in. It will either
// update the existing instance in place, or add the message
// instance to the scope's instance
// returns a bool indicating that the instance was actually
// accepted (and not skipped), and an error, if applicable
func (s *Scope) acceptInstance(instance *Instance) (bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.acceptInstanceUnsafe(instance)
}

func (s *Scope) sendAccept(instance *Instance, replicas []node.Node) error {
	oldTimeout := PREACCEPT_TIMEOUT
	PREACCEPT_TIMEOUT = 50
	defer func() { PREACCEPT_TIMEOUT = oldTimeout }()

	// send the message
	recvChan := make(chan *AcceptResponse, len(replicas))
	msg := &AcceptRequest{Scope: s.name, Instance: instance}
	sendMsg := func(n node.Node) {
		if response, err := n.SendMessage(msg); err != nil {
			logger.Warning("Error receiving PreAcceptResponse: %v", err)
		} else {
			if accept, ok := response.(*AcceptResponse); ok {
				recvChan <- accept
			} else {
				logger.Warning("Unexpected Accept response type: %T", response)
			}
		}
	}
	for _, replica := range replicas {
		go sendMsg(replica)
	}

	// receive the replies
	numReceived := 1 // this node counts as a response
	quorumSize := ((len(replicas) + 1) / 2) + 1
	timeoutEvent := time.After(time.Duration(ACCEPT_TIMEOUT) * time.Millisecond)
	var response *AcceptResponse
	responses := make([]*AcceptResponse, 0, len(replicas))
	for numReceived < quorumSize {
		select {
		case response = <-recvChan:
			responses = append(responses, response)
			numReceived++
		case <-timeoutEvent:
			return NewTimeoutError("Timeout while awaiting accept responses")
		}
	}

	// check if any of the messages were rejected
	accepted := true
	for _, response := range responses {
		accepted = accepted && response.Accepted
	}

	// handle rejected pre-accept messages
	if !accepted {
		// update max ballot from responses
		bmResponses := make([]BallotMessage, len(responses))
		for i, response := range responses {
			bmResponses[i] = BallotMessage(response)
		}
		s.updateInstanceBallotFromResponses(instance, bmResponses)
		// TODO: figure out what to do here. Try again?
		panic("rejected accept not handled yet")
	}
	return nil
}

// sets the given instance as committed
// in the case of handling messages from leaders to replicas
// the message instance should be passed in. It will either
// update the existing instance in place, or add the message
// instance to the scope's instance
// returns a bool indicating that the instance was actually
// accepted (and not skipped), and an error, if applicable
func (s *Scope) commitInstanceUnsafe(instance *Instance) (bool, error) {
	if existing, exists := s.instances[instance.InstanceID]; exists {
		if existing.Status >= INSTANCE_COMMITTED {
			return false, nil
		} else {
			// this replica may have missed an accept message
			// so copy the seq & deps onto the existing instance
			existing.Status = INSTANCE_COMMITTED
			existing.Dependencies = instance.Dependencies
			existing.Sequence = instance.Sequence
			existing.MaxBallot = instance.MaxBallot
		}
	} else {
		s.instances.Add(instance)
	}

	instance.Status = INSTANCE_COMMITTED
	s.inProgress.Remove(instance)
	s.committed.Add(instance)

	if instance.Sequence > s.maxSeq {
		s.maxSeq = instance.Sequence
	}

	if err := s.Persist(); err != nil {
		return false, err
	}

	return true, nil
}

// sets the given instance as committed
// in the case of handling messages from leaders to replicas
// the message instance should be passed in. It will either
// update the existing instance in place, or add the message
// instance to the scope's instance
// returns a bool indicating that the instance was actually
// accepted (and not skipped), and an error, if applicable
func (s *Scope) commitInstance(instance *Instance) (bool, error) {
	s.lock.Lock()
	s.lock.Unlock()

	return s.commitInstanceUnsafe(instance)
}

// Mark the instance as committed locally, persist state to disk, then send
// commit messages to the other replicas
// We're not concerned with whether the replicas actually respond because, since
// a quorum of nodes have agreed on the dependency graph for this instance, they
// won't be able to do anything else without finding out about it. This method
// will only return an error if persisting the committed state fails
func (s *Scope) sendCommit(instance *Instance, replicas []node.Node) error {
	msg := &CommitRequest{Scope: s.name, Instance: instance}
	sendCommit := func(n node.Node) { n.SendMessage(msg) }
	for _, replica := range replicas {
		go sendCommit(replica)
	}
	return nil
}

// applies an instance to the store
// first it will resolve all dependencies, then wait for them to commit/reject, or force them
// to do one or the other. Then it will execute it's committed dependencies, then execute itself
func (s *Scope) executeInstance(instance *Instance, replicas []node.Node) (store.Value, error) {
	return nil, nil
}

// increments the instance ballot, and sends and explicit prepare request
// to other nodes. The increment/send should happen in a lock, the receive should not.
// We don't want an incoming prepare request to increment the ballot before sending
// a message out to the other replicas
func (s *Scope) sendPrepare(instance *Instance) (<-chan *PrepareResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	instance.MaxBallot++
	if err := s.Persist(); err != nil {
		return nil, nil
	}
	recvChan := make(chan *PreAcceptResponse, 10)

	return recvChan, nil
}

// runs explicit prepare phase on instances where a command leader failure is suspected
// during execution,
// TODO:
//	what happens if 2 nodes send each other prepare messages at the same time?
func (s *Scope) prepareInstance(instance *Instance) error {

	// update instance ballot
	updateBallot := func() error {
		s.lock.Lock()
		defer s.lock.Unlock()
		instance.MaxBallot++
		if err := s.Persist(); err != nil {
			return err
		}
		return nil
	}
	if err := updateBallot(); err != nil {
		return err
	}

	// increment the instance ballot, and send prepare messages
	// to the replicas. By incrementing the instance ballot the
	// leader is attempting to take control of the instance if
	// more than one instance is attempting an explicit prepare
	// (which is likely) the first one to get a quorum of accept
	// messages wins.

	return nil
}

// executes a serialized query against the cluster
// this method designates the node it's called on as the command leader for the given query
// and therefore, should only be called once per client query
func (s *Scope) ExecuteQuery(instructions []*store.Instruction, replicas []node.Node) (store.Value, error) {
	// replica setup
	remoteReplicas := make([]node.Node, 0, len(replicas)-1)
	localReplicaFound := false
	for _, replica := range replicas {
		if replica.GetId() != s.GetLocalID() {
			remoteReplicas = append(remoteReplicas, replica)
		} else {
			localReplicaFound = true
		}
	}
	if !localReplicaFound {
		return nil, fmt.Errorf("Local replica not found in replica list, is this node a replica of the specified key?")
	}
	if len(remoteReplicas) != len(replicas)-1 {
		return nil, fmt.Errorf("remote replica size != replicas - 1. Are there duplicates?")
	}

	// create epaxos instance, and preaccept locally
	instance := s.makeInstance(instructions)
	if success, err := s.preAcceptInstance(instance); err != nil {
		return nil, err
	} else if !success {
		panic("instance already exists")
	}

	// send instance pre-accept to replicas
	paResponses, err := s.sendPreAccept(instance, remoteReplicas)
	if err != nil {
		return nil, err
	}

	if changes, err := s.mergePreAcceptAttributes(instance, paResponses); err != nil {
		return nil, err

	} else if changes {
		// some of the instance attributes received from the other replicas
		// were different from what was sent to them. Run the multi-paxos
		// accept phase

		// mark the local instance as accepted
		if err := s.setInstanceStatus(instance, INSTANCE_ACCEPTED); err != nil {
			return nil, err
		}
		if err := s.sendAccept(instance, replicas); err != nil {
			return nil, err
		}
	}

	// if we've gotten this far, either all the pre accept instance attributes
	// matched what was sent to them, or the correcting accept phase was successful
	// commit this instance
	if success, err := s.commitInstance(instance); err != nil {
		return nil, err
	} else if !success {
		panic("instance already exists")
	}
	s.sendCommit(instance, replicas)

	return s.executeInstance(instance, replicas)
}

// handles a preaccept message from the command leader for an instance
// this executes the replica preaccept phase for the given instance
func (s *Scope) HandlePreAccept(request *PreAcceptRequest) (*PreAcceptResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	extSeq := request.Instance.Sequence
	extDeps := NewInstanceIDSet(request.Instance.Dependencies)

	instance := request.Instance
	if success, err := s.preAcceptInstanceUnsafe(instance); err != nil {
		return nil, err
	} else if !success {
		panic("handling previously seen instance not handled yet")
	}

	// check agreement on seq and deps with leader
	newDeps := NewInstanceIDSet(instance.Dependencies)
	instance.dependencyMatch = extSeq == instance.Sequence && extDeps.Equal(newDeps)

	if err := s.Persist(); err != nil {
		return nil, err
	}

	missingDeps := newDeps.Subtract(extDeps)
	reply := &PreAcceptResponse{
		Accepted:         true,
		MaxBallot:        instance.MaxBallot,
		Instance:         instance,
		MissingInstances: make([]*Instance, 0, len(missingDeps)),
	}

	for iid := range missingDeps {
		inst := s.instances[iid]
		if inst != nil {
			reply.MissingInstances = append(reply.MissingInstances, inst)
		}
	}

	return reply, nil
}

// handles an accept message from the command leader for an instance
// this executes the replica accept phase for the given instance
func (s *Scope) HandleAccept(request *AcceptRequest) (*AcceptResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if instance, exists := s.instances[request.Instance.InstanceID]; exists {
		if instance.MaxBallot >= request.Instance.MaxBallot {
			return &AcceptResponse{Accepted: false, MaxBallot: instance.MaxBallot}, nil
		}
	}

	if success, err := s.acceptInstanceUnsafe(request.Instance); err != nil {
		return nil, err
	} else if !success {
		panic("handling previously seen instance not handled yet")
	}

	if len(request.MissingInstances) > 0 {
		s.addMissingInstancesUnsafe(request.MissingInstances...)
	}

	if err := s.Persist(); err != nil {
		return nil, err
	}
	return &AcceptResponse{Accepted: true}, nil
}

// handles an commit message from the command leader for an instance
// this executes the replica commit phase for the given instance
func (s *Scope) HandleCommit(request *CommitRequest) (*CommitResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if success, err := s.commitInstanceUnsafe(request.Instance); err != nil {
		return nil, err
	} else if !success {
		panic("handling previously seen instance not handled yet")
	}

	return &CommitResponse{}, nil
}

