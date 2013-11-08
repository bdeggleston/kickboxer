package store

import (
	"bufio"
	"time"
)

// enum indicating type of value
type ValueType string

type Value interface {
	// returns the highest level timestamp for this value
	GetTimestamp() time.Time
	GetValueType() ValueType

	Serialize(buf *bufio.Writer) error
	Deserialize(buf *bufio.Reader) error
}

type Instruction struct {
	Cmd string
	Key string
	Args []string
	Timestamp time.Time
}

type Store interface {

	Start() error
	Stop() error

	// ----------- queries -----------

	// executes a read instruction against the node's store
	ExecuteRead(cmd string, key string, args []string) (Value, error)

	// executes a write instruction against the node's store
	ExecuteWrite(cmd string, key string, args []string, timestamp time.Time) (Value, error)

	// reconciles multiple values and returns instructions for correcting
	// the values on inaccurate nodes
	Reconcile(values map[string] Value) (Value, map[string][]*Instruction, error)

	IsReadCommand(cmd string) bool
	IsWriteCommand(cmd string) bool

	// ----------- data import / export -----------

	// serializes a value
	SerializeValue(v Value) ([]byte, error)

	// serializes a value
	DeserializeValue(b []byte) (Value, ValueType, error)

	// returns raw data associated with the given key
	GetRawKey(key string) (Value, error)

	// sets the contents of the given key
	SetRawKey(key string, val Value) error

	// returns all of the keys held by the store
	GetKeys() []string
}

