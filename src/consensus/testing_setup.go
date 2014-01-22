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
	"store"
)

func setBreakpoint() {
	runtime.Breakpoint()
}

func setupScope() *Scope {
	manager := NewManager(newMockCluster())
	scope := NewScope("a", manager)
	seq := uint64(0)
	for i := 0; i < 4; i++ {
		seq++
		instance := scope.makeInstance(getBasicInstruction())
		instance.Status = INSTANCE_EXECUTED
		instance.Sequence = seq
		scope.instances.Add(instance)
		scope.executed = append(scope.executed, instance.InstanceID)
	}
	for i := 0; i < 4; i++ {
		seq++
		instance := scope.makeInstance(getBasicInstruction())
		instance.Status = INSTANCE_COMMITTED
		instance.Sequence = seq
		scope.instances.Add(instance)
		scope.committed.Add(instance)
	}
	for i := 0; i < 4; i++ {
		seq++
		instance := scope.makeInstance(getBasicInstruction())
		if i > 1 {
			instance.Status = INSTANCE_ACCEPTED
		} else {
			instance.Status = INSTANCE_PREACCEPTED
		}
		instance.Sequence = seq
		scope.instances.Add(instance)
		scope.inProgress.Add(instance)
	}
	return scope
}

// returns a set of mock nodes of the given size
func setupReplicaSet(size int) []*mockNode {
	replicas := make([]*mockNode, size)
	for i := 0; i < size; i++ {
		replicas[i] = newMockNode()
	}
	return replicas
}

func getBasicInstruction() []*store.Instruction {
	return []*store.Instruction{
		store.NewInstruction("set", "a", []string{"b", "c"}, time.Now()),
	}
}

// ...copies an instance. This is helpful when simulating
// message sending which are never serialized/deserialized
// so the 'remote' node isn't given a pointer to the actual
// leader instance it's working on, and field which aren't
// supposed to be serialized are nulled out
func copyInstance(i *Instance) *Instance {
	n := &Instance{}
	n.InstanceID = i.InstanceID
	n.LeaderID = i.LeaderID
	n.Sequence = i.Sequence
	n.Status = i.Status
	n.MaxBallot = i.MaxBallot
	n.Commands = make([]*store.Instruction, len(i.Commands))
	copy(n.Commands, i.Commands)
	n.Dependencies = make([]InstanceID, len(i.Dependencies))
	copy(n.Dependencies, i.Dependencies)
	return n
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
		Commands:     getBasicInstruction(),
		Dependencies: deps,
		Sequence:     0,
		Status:       INSTANCE_PREACCEPTED,
	}
	return instance
}

type baseScopeTest struct {
	cluster *mockCluster
	manager *Manager
	scope *Scope
}

func (s *baseScopeTest) getInstructions(values ...int) []*store.Instruction {
	instructions := make([]*store.Instruction, len(values))
	for i, val := range values {
		instructions[i] = store.NewInstruction("set", "a", []string{fmt.Sprintf("%v", val)}, time.Now())
	}
	return instructions
}

func (s *baseScopeTest) SetUpTest(c *gocheck.C) {
	s.cluster = newMockCluster()
	s.manager = NewManager(s.cluster)
	s.scope = NewScope("a", s.manager)
}

type baseReplicaTest struct {
	baseScopeTest
	nodes []*mockNode
	leader *mockNode
	replicas []*mockNode

	numNodes int
}

func (s *baseReplicaTest) SetUpSuite(c *gocheck.C) {
	s.numNodes = 5
}

func (s *baseReplicaTest) SetUpTest(c *gocheck.C) {
	c.Assert(s.numNodes > 2, gocheck.Equals, true)
	s.nodes = setupReplicaSet(s.numNodes)
	s.leader = s.nodes[0]
	s.replicas = s.nodes[1:]

	s.manager = s.leader.manager
	s.cluster = s.manager.cluster.(*mockCluster)
	s.scope = s.manager.getScope("a")
}
