package store

import (
	"bufio"
	"fmt"
	"strings"
	"sync"
	"time"
)

import (
	"serializer"
)

const (
	SINGLE_VALUE = ValueType("SINGLE")
)

// a single value used for
// key/val types
type singleValue struct {
	data string
	time time.Time
}

// single value constructor
func newSingleValue(data string, time time.Time) *singleValue {
	v := &singleValue{
		data:data,
		time:time,
	}
	return v
}

func (v *singleValue) GetTimestamp() time.Time {
	return v.time
}

func (v *singleValue) GetValueType() ValueType {
	return SINGLE_VALUE
}

func (v *singleValue) Serialize(buf *bufio.Writer) error {
	if err := serializer.WriteFieldBytes(buf, []byte(v.data)); err != nil {
		return err
	}
	if err := serializer.WriteTime(buf, v.time); err != nil {
		return err
	}
	return nil
}

func (v *singleValue) Deserialize(buf *bufio.Reader) error {
	if val, err := serializer.ReadFieldBytes(buf); err != nil {
		return err
	} else {
		v.data = string(val)
	}

	if t, err := serializer.ReadTime(buf); err != nil {
		return err
	} else {
		v.time = t
	}
	return nil
}

// read instructions
const (
	GET		= "GET"
)

// write instructions
const (
	SET		= "SET"
	DEL		= "DEL"
)


type Redis struct {

	data map[string] Value

	// TODO: delete
	// temporary lock, used until
	// things are broken out into
	// goroutines
	lock sync.RWMutex

}

func (s *Redis) Start() error {
	return nil
}

func (s *Redis) Stop() error {
	return nil
}

func (s *Redis) ExecuteRead(cmd string, key string, args []string) (*Value, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	switch cmd {
	case GET:
		//
	default:
		return nil, fmt.Errorf("Unrecognized read command: %v", cmd)
	}

	return nil, nil
}

func (s *Redis) ExecuteWrite(cmd string, key string, args []string, timestamp time.Time) (*Value, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	switch cmd {
	case SET:
		//
	case DEL:
		//
	default:
		return nil, fmt.Errorf("Unrecognized read command: %v", cmd)
	}
	return nil, nil
}

func (s *Redis) IsReadCommand(cmd string) bool {
	switch strings.ToUpper(cmd) {
	case GET:
		return true
	}
	return false
}

func (s *Redis) IsWriteCommand(cmd string) bool {
	switch strings.ToUpper(cmd) {
	case SET, DEL:
		return true
	}
	return false
}
