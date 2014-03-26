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

	// compares 2 Values
	Equal(value Value) bool
}

type Store interface {

	Start() error
	Stop() error

	// ----------- queries -----------

	// executes a query instruction against the node's store
	ExecuteQuery(cmd string, key string, args []string, timestamp time.Time) (Value, error)

	// reconciles multiple values and returns instructions for correcting
	// the values on inaccurate nodes
	Reconcile(key string, values map[string] Value) (Value, map[string][]*Instruction, error)

	// determines if the given command returns a value
	ReturnsValue(cmd string) bool

	// returns an array of keys used by consensus to determine
	// which instructions the given instruction set will interfere
	// with. Basically, think of the given instructions as operating
	// on a nested hash, the reurned array of keys should describe the
	// hierarchy of keys being operated on.
	// ie: store['a']['b']['c'] = 5 returns ['a', 'b', 'c'] as the interfering keys
	//
	// the instance will gain dependencies on instances in the parent and
	// child keys, but not on siblings.
	InterferingKeys(instruction *Instruction) []string

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

	// checks if a key exists in the store
	KeyExists(key string) bool
}

