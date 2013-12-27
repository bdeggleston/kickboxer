package consensus

import (
	"fmt"
	"sync"
)

import (
	"node"
	"store"
)

// manages a subset of interdependent
// consensus operations
type Scope struct {
	name        string
	instanceMap map[InstanceID]*Instance
	inProgress  map[InstanceID]*Instance
	committed   map[InstanceID]*Instance
	executed    []InstanceID
	maxSeq      uint64
	lock        sync.RWMutex
	manager		*Manager
}

func NewScope(name string, manager *Manager) *Scope {
	return &Scope{
		name:        name,
		instanceMap: make(map[InstanceID]*Instance),
		inProgress:  make(map[InstanceID]*Instance),
		committed:   make(map[InstanceID]*Instance),
		executed:    make([]*Instance, 0, 16),
		manager:     manager,
	}
}

func (s *Scope) GetLocalID() node.NodeId {
	return s.manager.GetLocalID()
}

func (s *Scope) ExecuteInstructions(instructions []*store.Instruction, replicas []node.Node) (store.Value, error) {
	// replica setup
	remoteReplicas := make([]node.Node, 0, len(replicas) - 1)
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
	if len(remoteReplicas) != len(replicas) - 1 {
		return nil, fmt.Errorf("remote replica size != replicas - 1. Are there duplicates?")
	}


	return nil, nil
}
