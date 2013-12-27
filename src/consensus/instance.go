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

const (
	DEPENDENCY_HORIZON = 5
)

type InstanceID string

func NewInstanceID() InstanceID {
	return InstanceID(uuid.NewUUID())
}

func (i *InstanceID) UUID() uuid.UUID {
	return uuid.UUID(*i)
}

func (i *InstanceID) String() string {
	return i.UUID().String()
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
	// *not serialized
	commitTimeout time.Time

	// indicates that the dependencies from the leader
	// matched the replica's local dependencies. This
	// is used when there are only 3 replicas and another
	// replica takes over leadership for a command.
	// Even if the old command leader is unreachable,
	// the new leader will know that at least 2 replicas
	// had identical dependency graphs at the time of proposal,
	// it may still be useful in other situations
	// *not serialized
	dependencyMatch bool
}


