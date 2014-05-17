package consensus

import (
	"fmt"
	"runtime"
	"time"
)

import (
	"launchpad.net/gocheck"
)

import (
	"node"
	"partitioner"
	"store"
	"topology"
)

func setBreakpoint() {
	runtime.Breakpoint()
}

func setupDeps(manager *Manager) {
	seq := uint64(0)
	for i := 0; i < 4; i++ {
		seq++
		instance := manager.makeInstance(getBasicInstruction())
		instance.Status = INSTANCE_EXECUTED
		instance.Dependencies, _ = manager.getInstanceDeps(instance)
		manager.instances.Add(instance)
		manager.executed = append(manager.executed, instance.InstanceID)
	}
	for i := 0; i < 4; i++ {
		seq++
		instance := manager.makeInstance(getBasicInstruction())
		instance.Status = INSTANCE_COMMITTED
		instance.Dependencies, _ = manager.getInstanceDeps(instance)
		manager.instances.Add(instance)
	}
	for i := 0; i < 4; i++ {
		seq++
		instance := manager.makeInstance(getBasicInstruction())
		if i > 1 {
			instance.Status = INSTANCE_ACCEPTED
		} else {
			instance.Status = INSTANCE_PREACCEPTED
		}
		instance.Dependencies, _ = manager.getInstanceDeps(instance)
		manager.instances.Add(instance)
	}
}

func setupManager() *Manager {
	manager := NewManager(
		topology.NewTopology(
			node.NewNodeId(),
			topology.DatacenterID("DC1"),
			partitioner.NewMD5Partitioner(),
			3,
		),
		newMockStore(),
	)
	setupDeps(manager)
	return manager
}

// returns a set of mock nodes of the given size
func setupReplicaSet(size int) []*mockNode {
	replicas := make([]*mockNode, size)
	for i := 0; i < size; i++ {
		replicas[i] = newMockNode()
	}
	nodes := make([]node.Node, size)
	for i:=0; i<size; i++ {
		nodes[i] = replicas[i]
	}

	for _, n1 := range replicas {
		// TODO: make the replication factor a mock node constructor param?
		n1.manager.topology = topology.NewTopology(
			n1.id,
			n1.dcID,
			partitioner.NewMD5Partitioner(),
			uint(size),
		)
		for _, n2 := range replicas {
			n1.manager.topology.AddNode(n2)
		}
	}

	return replicas
}

func getBasicInstruction() store.Instruction {
	return store.NewInstruction("set", "a", []string{"b", "c"}, time.Now())
}

func copyDependencies(o []*InstanceID) []*InstanceID {
	n := make([]*InstanceID, len(o))
	copy(n, o)
	return n
}

func makeDependencies(size int) []InstanceID {
	d := make([]InstanceID, size)
	for i := 0; i < size; i++ {
		d[i] = NewInstanceID()
	}
	return d
}

func makeInstance(nid node.NodeId, deps []InstanceID) *Instance {
	instance := &Instance{
		InstanceID:   NewInstanceID(),
		LeaderID:     nid,
		Command:      getBasicInstruction(),
		Dependencies: deps,
		Status:       INSTANCE_PREACCEPTED,
		Successors:   make([]node.NodeId, 0),
	}
	return instance
}

type baseManagerTest struct {
	manager *Manager
}

func (s *baseManagerTest) getInstruction(val int) store.Instruction {
	return store.NewInstruction("set", "a", []string{fmt.Sprintf("%v", val)}, time.Now())
}

func (s *baseManagerTest) SetUpTest(c *gocheck.C) {
	s.manager = NewManager(
		topology.NewTopology(
			node.NewNodeId(),
			topology.DatacenterID("DC1"),
			partitioner.NewMD5Partitioner(),
			3,
		),
		newMockStore(),
	)
	s.manager.stats = newMockStatter()
}

type baseReplicaTest struct {
	baseManagerTest
	nodes []*mockNode
	managers []*Manager
	replicaManagers []*Manager
	leader *mockNode
	replicas []*mockNode
	nodeMap map[node.NodeId]*mockNode

	numNodes int
}

func (s *baseReplicaTest) quorumSize() int {
	return (s.numNodes / 2) + 1
}

func (s *baseReplicaTest) SetUpSuite(c *gocheck.C) {
	s.numNodes = 5
}

func (s *baseReplicaTest) SetUpTest(c *gocheck.C) {
	c.Assert(s.numNodes > 2, gocheck.Equals, true)
	s.nodes = setupReplicaSet(s.numNodes)
	s.managers = make([]*Manager, s.numNodes)
	s.replicaManagers = make([]*Manager, s.numNodes - 1)

	s.nodeMap = make(map[node.NodeId]*mockNode, s.numNodes)
	for _, n := range s.nodes {
		s.nodeMap[n.id] = n
	}

	s.leader = s.nodes[0]
	s.replicas = s.nodes[1:]

	s.manager = s.leader.manager
	s.manager.stats = newMockStatter()
	for i, n := range s.nodes {
		s.managers[i] = n.manager
	}
	for i, n := range s.replicas {
		s.replicaManagers[i] = n.manager
	}
}
