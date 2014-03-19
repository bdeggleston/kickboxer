package consensus

import (
	"fmt"
	"sync"
)

import (
	"github.com/cactus/go-statsd-client/statsd"
)

import (
	cluster "clusterproto"
	"message"
	"node"
	"store"
)

// replica level manager for consensus operations
type Manager struct {
	lock     sync.RWMutex
	cluster  cluster.Cluster
	stats    statsd.Statter

	instances    *InstanceMap
	inProgress   *InstanceMap
	committed    *InstanceMap

	executed     []InstanceID
	executedLock sync.RWMutex

	maxSeq       uint64
	maxSeqLock   sync.RWMutex
}

func NewManager(cluster cluster.Cluster) *Manager {
	stats, _ := statsd.NewNoop()
	return &Manager{
		cluster:  cluster,
		stats:    stats,
		instances:  NewInstanceMap(),
		inProgress: NewInstanceMap(),
		committed:  NewInstanceMap(),
		executed:   make([]InstanceID, 0, 16),
	}
}

func (m *Manager) setStatter(s statsd.Statter) {
	if s == nil {
		panic("cannot set nil statter")
	}
	m.stats = s
}

// returns the replicas for the scope's key, at the given  consistency level
func (m *Manager) getInstanceNodes(instance *Instance) []node.Node {
	return m.cluster.GetNodesForKey(instance.Commands[0].Key)
}

func (m *Manager) checkLocalKeyEligibility(key string) bool {
	nodes := m.cluster.GetNodesForKey(key)
	localID := m.GetLocalID()
	for _, n := range nodes {
		if n.GetId() == localID {
			return true
		}
	}
	return false
}

func (m *Manager) checkLocalInstanceEligibility(instance *Instance) bool {
	return m.checkLocalKeyEligibility(instance.Commands[0].Key)
}

// returns the replicas for the given instance's key, excluding the local node
func (m *Manager) getInstanceReplicas(instance *Instance) []node.Node {
	nodes := m.cluster.GetNodesForKey(instance.Commands[0].Key)
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

func (m *Manager) ExecuteQuery(instructions []*store.Instruction) (store.Value, error) {
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

	if !m.checkLocalKeyEligibility(key) {
		// need to iterate over the possible replicas, allowing for
		// some to be down
		panic("Forward to eligible replica not implemented yet")
	} else {
		scope := m.getScope(key)
		val, err := scope.ExecuteQuery(instructions)
		return val, err
	}

	return nil, nil
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
		return nil, fmt.Errorf("Unhandled scoped request type: %T", request)
	}
	panic("unreachable")
}
