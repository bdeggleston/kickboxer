package cluster

import (
	"store"
)

type CommandStatus byte

const (
	DS_NULL			= CommandStatus(iota)
	DS_PRE_ACCEPT
	DS_ACCEPTED
	DS_REJECTED
	DS_COMMITTED
	DS_EXECUTED
)

type Command struct {
	// the node id of the command leader
	leader NodeId

	// the current status of this command
	status CommandStatus

	// the actual instruction to be executed
	instruction *store.Instruction

	// indicates that previous commands need
	// to be executed before a decision can be
	// made for this command
	require_execution bool

	// the ballot number for this commend
	ballot uint64
}

// manager for interfering commands
type Instance struct {
	dependencies []Command
	max_ballot uint64
	scope string
}

func (c *Cluster) executeConsensusInstruction(instruction store.Instruction, execution_required bool) {

}

func (c *Cluster) handlePreAccept(scope string, cmd *Command) {

}
