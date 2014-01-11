package consensus

import (
	"fmt"
	"sync"
)

import (
	cluster "clusterproto"
	"message"
	"node"
	"store"
)

// replica level manager for consensus operations
type Manager struct {
	scopeMap map[string]*Scope
	lock     sync.RWMutex
	cluster  cluster.Cluster
}

func NewManager(cluster cluster.Cluster) *Manager {
	return &Manager{
		scopeMap: make(map[string]*Scope),
		cluster:  cluster,
	}
}

func (m *Manager) getScope(key string) *Scope {
	// get
	m.lock.RLock()
	instance, exists := m.scopeMap[key]
	m.lock.RUnlock()

	// or create
	if !exists {
		m.lock.Lock()
		instance = NewScope(key, m)
		m.scopeMap[key] = instance
		m.lock.Unlock()
	}

	return instance
}

// returns the replicas for the scope's key, at the given  consistency level
func (m *Manager) getScopeNodes(s *Scope) []node.Node {
	return m.cluster.GetNodesForKey(s.name)
}

func (m *Manager) checkLocalScopeEligibility(s *Scope) bool {
	nodes := m.getScopeNodes(s)
	localID := m.GetLocalID()
	for _, n := range nodes {
		if n.GetId() == localID {
			return true
		}
	}
	return false
}

// returns the replicas for the given scope's key, excluding the local node
func (m *Manager) getScopeReplicas(s *Scope) []node.Node {
	nodes := m.getScopeNodes(s)
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

func (m *Manager) ExecuteQuery(instructions []*store.Instruction, replicas []node.Node) (store.Value, error) {
	localId := m.cluster.GetID()
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
		val, err := scope.ExecuteQuery(instructions, replicas)
		return val, err
	}

	return nil, nil
}

func (m *Manager) HandleMessage(msg message.Message) (message.Message, error) {
	if scopedRequest, ok := msg.(ScopedMessage); ok {
		scope := m.getScope(scopedRequest.GetScope())
		switch request := scopedRequest.(type) {
		case *PreAcceptRequest:
			return scope.HandlePreAccept(request)
		case *AcceptRequest:
			return scope.HandleAccept(request)
		case *CommitRequest:
			return scope.HandleCommit(request)
		default:
			return nil, fmt.Errorf("Unhandled scoped request type: %T", scopedRequest)

		}
	} else {
		return nil, fmt.Errorf("Only scoped messages are handled")
	}
	panic("unreachable")
}
