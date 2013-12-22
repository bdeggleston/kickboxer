package cluster

import (
	"time"
)

import (
	"store"
)

type CommandStatus byte

const (
	DS_NULL = CommandStatus(iota)
	DS_PRE_ACCEPT
	DS_ACCEPTED
	DS_REJECTED
	DS_COMMITTED
	DS_EXECUTED
)

// TODO: should consensus operations hijack the timestamp??
// TODO: should reads and writes be collapsed into a single function? Let the store decide what to do?

type Command struct {
	// the node id of the command leader
	LeaderID NodeId

	// the current status of this command
	Status CommandStatus

	// the actual instruction to be executed
	Cmd string
	Key string
	Args []string
	Timestamp time.Time

	// indicates that previous commands need
	// to be executed before a decision can be
	// made for this command
	Blocking bool

	// the ballot number for this commend
	Ballot uint64
}

// manager for interfering commands
type Instance struct {
	Dependencies []Command
	MaxBallot   uint64
}


func (c *Cluster) executeConsensusInstruction(instruction store.Instruction, consistency ConsistencyLevel) {

	// check if this node can be the comamnd leader
	localReplicas := c.GetLocalNodesForKey(instruction.Key)
	eligibleLeader := false
	for _, replica := range localReplicas {
		if replica.GetId() == c.GetNodeId() {
			eligibleLeader = true
		}
	}
	if !eligibleLeader {
		panic("Forward to key replica")
	} else {

	}
}

func (c *Cluster) handlePreAccept(cmd *Command, dependencies []Command) {

}
