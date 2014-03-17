package node

import (
	"time"
)

import (
	"message"
	"store"
	"types"
)

type NodeId struct {
	types.UUID
}

func NewNodeId() NodeId {
	return NodeId{types.NewUUID1()}
}

type NodeError struct {
	reason string
}

func NewNodeError(reason string) *NodeError {
	return &NodeError{reason:reason}
}

func (e *NodeError) Error() string {
	return e.reason
}

// the basic node interface
type Node interface {
	GetId() NodeId

	ExecuteQuery(cmd string, key string, args []string, timestamp time.Time) (store.Value, error)
	SendMessage(message.Message) (message.Message, error)
}
