package consensus

import (
	"time"
)

import (
	"node"
	"store"
)

import (
	"code.google.com/p/go-uuid/uuid"
)

type InstanceStatus byte

const (
	INSTANCE_IGNORED = InstanceStatus(iota)
	INSTANCE_PREACCEPTED
	INSTANCE_ACCEPTED
	INSTANCE_REJECTED
	INSTANCE_COMMITTED
	INSTANCE_EXECUTED
)

type InstanceID string

func NewInstanceID() InstanceID {
	return InstanceID(uuid.NewUUID())
}

func (i InstanceID) UUID() uuid.UUID {
	return uuid.UUID(i)
}

func (i InstanceID) String() string {
	return i.UUID().String()
}

type InstanceIDSet map[InstanceID]bool

func NewInstanceIDSet(ids []InstanceID) InstanceIDSet {
	s := make(InstanceIDSet, len(ids))
	for _, id := range ids {
		s[id] = true
	}
	return s
}

func (i InstanceIDSet) Equal(o InstanceIDSet) bool {
	if len(i) != len(o) {
		return false
	}
	for key := range i {
		if !o[key] {
			return false
		}
	}
	return true
}

func (i InstanceIDSet) Union(o InstanceIDSet) InstanceIDSet {
	u := make(InstanceIDSet, (len(i) * 3) / 2)
	for k := range i {
		u[k] = true
	}
	for k := range o {
		u[k] = true
	}
	return u
}

type Instance struct {
	// the uuid of this instance
	InstanceID InstanceID

	// the node id of the instance leader
	LeaderID node.NodeId

	// the Instructions(s) to be executed
	Commands []*store.Instruction

	Dependencies []InstanceID

	// the sequence number of this instance (like an array index)
	Sequence uint64

	// the current status of this instance
	Status InstanceStatus

	// the highest seen message number for this instance
	MaxBallot uint32

	// indicates the time that we can stop waiting
	// for a commit on this command, and force one
	// * not message serialized *
	commitTimeout time.Time

	// indicates that the dependencies from the leader
	// matched the replica's local dependencies. This
	// is used when there are only 3 replicas and another
	// replica takes over leadership for a command.
	// Even if the old command leader is unreachable,
	// the new leader will know that at least 2 replicas
	// had identical dependency graphs at the time of proposal,
	// it may still be useful in other situations
	// * not message serialized *
	dependencyMatch bool
}

// merges sequence and dependencies onto this instance, and returns
// true/false to indicate if there were any changes
func (i *Instance) mergeAttributes(seq uint64, deps []InstanceID) bool {
	changes := false
	if seq > i.Sequence {
		changes = true
		i.Sequence = seq
	}
	iSet := NewInstanceIDSet(i.Dependencies)
	oSet := NewInstanceIDSet(deps)
	if !iSet.Equal(oSet) {
		changes = true
		union := iSet.Union(oSet)
		i.Dependencies = make([]InstanceID, 0, len(union))
		for id := range union {
			i.Dependencies = append(i.Dependencies, id)
		}
	}
	return changes
}
