package node

import (
	"time"
)

import (
	"code.google.com/p/go-uuid/uuid"
)

import (
	"message"
	"store"
)

type NodeId string

func NewNodeId() NodeId {
	return NodeId(uuid.NewUUID().String())
}

func (nid NodeId) IsNil() bool {
	return nid == NodeId("")
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
