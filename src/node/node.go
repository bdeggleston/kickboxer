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
	return NodeId(uuid.NewUUID())
}

func (i NodeId) UUID() uuid.UUID {
	return uuid.UUID(i)
}

func (i NodeId) String() string {
	return i.UUID().String()
}

func (i NodeId) IsNil() bool {
	return i == NodeId("")
}

func (i NodeId) MarshalJSON() ([]byte, error) {
	return []byte("\"" + i.String() + "\""), nil
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
