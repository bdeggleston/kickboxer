/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/2/13
 * Time: 7:07 PM
 * To change this template use File | Settings | File Templates.
 */
package store

import (
	"bufio"
//	"reflect"
	"time"
)

type Value interface {
	// returns the highest level timestamp for this value
	GetTimestamp() time.Time

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

	// serializes a value
	SerializeValue(v *Value) ([]byte, error)

	// serializes a value
	DeserializeValue(b []byte) (Value, error)

	// executes a read instruction against the node's store
	ExecuteRead(cmd string, key string, args []string) (*Value, error)

	// executes a write instruction against the node's store
	ExecuteWrite(cmd string, key string, args []string, timestamp time.Time) (*Value, error)

	// reconciles multiple values
	Reconcile(values map[string] *Value) (*Value, []*Instruction, error)

}

