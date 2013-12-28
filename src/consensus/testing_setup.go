package consensus

import (
	"time"
)

import (
	"store"
)

// returns a set of mock nodes of the given size
func setupReplicaSet(size int) []*mockNode {
	replicas := make([]*mockNode, size)
	for i:=0;i<size;i++ {
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
