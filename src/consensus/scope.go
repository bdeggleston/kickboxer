package consensus

import (
	"fmt"
	"sync"
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

// returns the current dependencies for a new instance
// this method doesn't implement any locking or persistence
func (s *Scope) getCurrentDeps() []InstanceID {
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
func (s *Scope) getNextSeq() uint64 {
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
		Dependencies: s.getCurrentDeps(),
		Sequence: s.getNextSeq(),
		Status: INSTANCE_PREACCEPTED,
		MaxBallot: 1,
	}

	// add to manager maps
	s.instances[instance.InstanceID] = instance
	s.inProgress[instance.InstanceID] = instance

	if err := s.Persist(); err != nil {
		return nil, err
	}

	return instance, nil
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
	preAcceptResponsChan := make(chan *PreAcceptResponse, len(remoteReplicas))
	msg := &PreAcceptRequest{Scope:s.name, Instance:instance}
	sendMsg := func(n node.Node) {
		if response, err := n.SendMessage(msg); err != nil {
			logger.Warning("Error receiving PreAcceptResponse: %v", err)
		} else {
			if preAccept, ok := response.(*PreAcceptResponse); !ok {
				logger.Warning("Unexpected PreAccept response type: %T", response)
				preAcceptResponsChan <- preAccept
			}
		}
	}
	for _, node := range remoteReplicas {
		go sendMsg(node)
	}

	_ = instance
	return nil, nil
}
