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
)

// manages a subset of interdependent
// consensus operations
type Scope struct {
	name       string
	instances  map[InstanceID]*Instance
	inProgress map[InstanceID]*Instance
	committed  map[InstanceID]*Instance
	executed   []InstanceID
	maxSeq     uint64
	lock       sync.RWMutex
	cmdLock    sync.Mutex
	manager    *Manager
}

func NewScope(name string, manager *Manager) *Scope {
	return &Scope{
		name:       name,
		instances:  make(map[InstanceID]*Instance),
		inProgress: make(map[InstanceID]*Instance),
		committed:  make(map[InstanceID]*Instance),
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
	// TODO: use fewer deps (inProgress + committed + executed[len(executed)-1])
	executedDeps := 1
	if len(s.executed) == 0 {
		executedDeps = 0
	}
	deps := make([]InstanceID, len(s.inProgress) + len(s.committed) + executedDeps)
	for dep := range s.inProgress { deps = append(deps, dep) }
	for dep := range s.committed { deps = append(deps, dep) }
	if executedDeps > 0 { deps = append(deps, s.executed[len(s.executed) - 1]) }

	return deps
}

// returns the next available sequence number for a new instance
// this method doesn't implement any locking or persistence
func (s *Scope) getNextSeqUnsafe() uint64 {
	s.maxSeq++
	return s.maxSeq
}

// creates an epaxos instance from the given instructions
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
	s.instances[instance.InstanceID] = instance
	s.inProgress[instance.InstanceID] = instance

	if err := s.Persist(); err != nil {
		return nil, err
	}

	return instance, nil
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
			if preAccept, ok := response.(*PreAcceptResponse); !ok {
				logger.Warning("Unexpected PreAccept response type: %T", response)
				recvChan <- preAccept
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
	responses := make([]*PreAcceptResponse, len(replicas))
	for numReceived < quorumSize {
		select {
		case response = <-recvChan:
			responses = append(responses, response)
			numReceived++
		case <-timeoutEvent:
			return nil, fmt.Errorf("Timeout while awaiting pre accept responses")
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
	preAcceptResponses, err := s.sendPreAccept(instance, remoteReplicas)
	if err != nil { return nil, err }

	return nil, nil
}
