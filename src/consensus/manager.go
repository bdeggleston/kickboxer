package consensus

import (
	"fmt"
	"sync"
)

import (
	"node"
	"store"
)

// replica level manager for consensus operations
type Manager struct {
	scopeMap map[string]*Scope
	lock sync.RWMutex
	coordinator NodeCoordinator
}

func NewManager(coordinator NodeCoordinator) *Manager {
	return &Manager{
		scopeMap: make(map[string]*Scope),
		coordinator:coordinator,
	}
}

func (m *Manager) getScope(key string) *Instance {
	// get
	m.lock.RLock()
	instance, exists := m.instances[key]
	m.lock.RUnlock()

	// or create
	if !exists {
		m.lock.Lock()
		instance = NewScope(key, m.cluster)
		m.instances[key] = instance
		m.lock.Unlock()
	}

	return instance
}
func (m *Manager) ExecuteInstructions(instructions []*store.Instruction, replicas []node.Node) (store.Value, error) {
	localId := m.coordinator.GetID()
	eligibleLeader := false
	for _, replica := range replicas {
		if replica.GetId() == localId {
			eligibleLeader = true
			break
		}
	}

	// check that all the instruction keys are the same
	if len(instructions) == 0 {
		return nil, fmt.Errorf("need at least one instruction to execute")
	}

	key := instructions[0].Key
	if len(instructions) > 1 {
		for _, instruction := range instructions[1:] {
			if instruction.Key != key {
				return nil, fmt.Errorf("Multiple keys found, each instruction must operate on the same key")
			}
		}
	}

	if !eligibleLeader {
		// need to iterate over the possible replicas, allowing for
		// some to be down
		panic("Forward to eligible replica not implemented yet")
	} else {
		scope := m.getScope(key)
		val, err := scope.ExecuteInstruction(instructions, replicas)
		return val, err
	}

	return nil, nil
}


