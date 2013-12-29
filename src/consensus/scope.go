package consensus

import (
	"fmt"
	"sync"
	"time"
)

import (
	"message"
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
)

func makePreAcceptCommitTimeout() time.Time {
	return time.Now().Add(time.Duration(PREACCEPT_COMMIT_TIMEOUT) * time.Millisecond)
}

func makeAcceptCommitTimeout() time.Time {
	return time.Now().Add(time.Duration(ACCEPT_COMMIT_TIMEOUT) * time.Millisecond)
}

type TimeoutError struct {
	message string
}

func (t TimeoutError) Error() string { return t.message }
func (t TimeoutError) String() string { return t.message }
func NewTimeoutError(format string, a ...interface{}) TimeoutError {
	return TimeoutError{fmt.Sprintf(format, a...)}
}

// manages a subset of interdependent
// consensus operations
type Scope struct {
	name       string
	instances  InstanceMap
	inProgress InstanceMap
	committed  InstanceMap
	executed   []InstanceID
	maxSeq     uint64
	lock       sync.RWMutex
	cmdLock    sync.Mutex
	manager    *Manager
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
	deps  = append(deps, s.inProgress.InstanceIDs()...)
	deps  = append(deps, s.committed.InstanceIDs()...)

	if len(s.executed) > 0 {
		deps = append(deps, s.executed[len(s.executed) - 1])
	}

	return deps
}

// returns the next available sequence number for a new instance
// this method doesn't implement any locking or persistence
func (s *Scope) getNextSeqUnsafe() uint64 {
	s.maxSeq++
	return s.maxSeq
}

// creates an epaxos instance from the given instructions and adds it
// to the scope's bookeeping containers
func (s *Scope) makeInstance(instructions []*store.Instruction) (*Instance, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	instance := &Instance{
		InstanceID: NewInstanceID(),
		LeaderID: s.GetLocalID(),
		Commands: instructions,
		Dependencies: s.getCurrentDepsUnsafe(),
		Sequence: s.getNextSeqUnsafe(),
		Status: INSTANCE_PREACCEPTED,
	}

	// add to manager maps
	s.instances.Add(instance)
	s.inProgress.Add(instance)

	if err := s.Persist(); err != nil {
		return nil, err
	}

	return instance, nil
}

func (s *Scope) addMissingInstanceUnsafe(instance *Instance) error {
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

// sends pre accept responses to the given replicas, and returns their responses. An error will be returned
// if there are problems, or a quorum of responses were not received within the timeout
func (s *Scope) sendPreAccept(instance *Instance, replicas []node.Node) ([]*PreAcceptResponse, error) {
	//
	recvChan := make(chan *PreAcceptResponse, len(replicas))
	msg := &PreAcceptRequest{Scope:s.name, Instance:instance}
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

	numReceived := 1  // this node counts as a response
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

func (s *Scope) sendAccept(instance *Instance, replicas []node.Node) error {


	// send the message
	recvChan := make(chan *AcceptResponse, len(replicas))
	msg := &AcceptRequest{Scope:s.name, Instance:instance}
	sendMsg := func(n node.Node) {
		if response, err := n.SendMessage(msg); err != nil {
			logger.Warning("Error receiving PreAcceptResponse: %v", err)
		} else {
			if accept, ok := response.(*AcceptResponse); !ok {
				logger.Warning("Unexpected Accept response type: %T", response)
				recvChan <- accept
			}
		}
	}
	for _, replica := range replicas {
		go sendMsg(replica)
	}

	// receive the replies
	numReceived := 1  // this node counts as a response
	quorumSize := ((len(replicas) + 1) / 2) + 1
	timeoutEvent := time.After(time.Duration(ACCEPT_TIMEOUT) * time.Millisecond)
	var response *AcceptResponse
	responses := make([]*AcceptResponse, len(replicas))
	for numReceived < quorumSize {
		select {
		case response = <-recvChan:
			responses = append(responses, response)
			numReceived++
		case <-timeoutEvent:
			return fmt.Errorf("Timeout while awaiting pre accept responses")
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

// Mark the instance as committed locally, persist state to disk, then send
// commit messages to the other replicas
// We're not concerned with whether the replicas actually respond because, since
// a quorum of nodes have agreed on the dependency graph for this instance, they
// won't be able to do anything else without finding out about it. This method
// will only return an error if persisting the committed state fails
func (s *Scope) commitInstance(instance *Instance, replicas []node.Node) error {
	if err := s.setInstanceStatus(instance, INSTANCE_COMMITTED); err != nil {
		return err
	}

	msg := &CommitRequest{Scope:s.name, InstanceID:instance.InstanceID}
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

func (s *Scope) ExecuteInstructions(instructions []*store.Instruction, replicas []node.Node) (store.Value, error) {
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

	// create epaxos instance
	instance, err := s.makeInstance(instructions)
	if err != nil { return nil, err }

	// send instance pre-accept to replicas
	paResponses, err := s.sendPreAccept(instance, remoteReplicas)
	if err != nil { return nil, err }

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
	s.commitInstance(instance, replicas)

	return s.executeInstance(instance, replicas)
}

func (s *Scope) HandlePreAccept(request *PreAcceptRequest) (*PreAcceptResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	id := request.Instance.InstanceID

	// TODO: check if this is a late message (ie: this replica already knows about the instance)
	if s.instances.ContainsID(id) {
		panic("handling previously seen instance not handled yet")
	}

	extSeq := request.Instance.Sequence
	extDeps := NewInstanceIDSet(request.Instance.Dependencies)

	instance := request.Instance
	instance.Status = INSTANCE_PREACCEPTED
	instance.Sequence = s.getNextSeqUnsafe()
	instance.Dependencies = s.getCurrentDepsUnsafe()
	instance.commitTimeout = makePreAcceptCommitTimeout()

	// add to scope
	s.instances.Add(instance)
	s.inProgress.Add(instance)

	// check agreement on seq and deps with leader
	newDeps := NewInstanceIDSet(instance.Dependencies)
	instance.dependencyMatch = extSeq == instance.Sequence && extDeps.Equal(newDeps)

	if err := s.Persist(); err != nil {
		return nil, err
	}

	missingDeps := newDeps.Subtract(extDeps)
	reply := &PreAcceptResponse{
		Accepted: true,
		MaxBallot: instance.MaxBallot,
		Instance: instance,
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

func (s *Scope) HandleAccept(request *AcceptRequest) (*AcceptResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	reqInstance := request.Instance
	id := reqInstance.InstanceID

	if !s.instances.ContainsID(id) {
		// add to scope
		reqInstance.commitTimeout = makeAcceptCommitTimeout()
		s.instances.Add(reqInstance)
		s.inProgress.Add(reqInstance)
	} else {
		instance := s.instances[id]

		// message is out of date, reject
		if instance.MaxBallot >= reqInstance.MaxBallot {
			return &AcceptResponse{Accepted:false, MaxBallot:instance.MaxBallot}, nil
		}

		if instance.Status != INSTANCE_PREACCEPTED {
			// how would this happen, if the ballot was correct?
			panic("handling non pre accept instance not handled yet")
		}

		// accept the new attributes
		instance.Dependencies = reqInstance.Dependencies
		instance.Sequence = reqInstance.Sequence
		instance.MaxBallot = reqInstance.MaxBallot
		instance.commitTimeout = makeAcceptCommitTimeout()
	}

	for _, instance := range request.MissingInstances {
		s.addMissingInstanceUnsafe(instance)
	}

	if err := s.Persist(); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *Scope) HandleCommit(request *CommitRequest) (*CommitResponse, error) {
	return nil, nil
}

func (s *Scope) HandleMessage(message ScopedMessage) (message.Message, error) {
	switch request := message.(type) {
	case *PreAcceptRequest:
		return s.HandlePreAccept(request)
	case *CommitRequest:
		return s.HandleCommit(request)
	case *AcceptRequest:
		return s.HandleAccept(request)
	default:
		return nil, fmt.Errorf("Unexpected message type: %T", message)
	}
	panic("unreachable")
}
