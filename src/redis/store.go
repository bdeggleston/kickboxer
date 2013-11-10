package redis

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"
)

import (
	"store"
)


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

	data map[string] store.Value

	// TODO: delete
	// temporary lock, used until
	// things are broken out into
	// goroutines
	lock sync.RWMutex

}

func NewRedis() *Redis {
	r := &Redis{
		data:make(map[string] store.Value),
	}
	return r
}

func (s *Redis) SerializeValue(v store.Value) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := WriteValue(buf, v) ; err != nil { return nil, err }
	return buf.Bytes(), nil
}

func (s *Redis) DeserializeValue(b []byte) (store.Value, store.ValueType, error) {
	buf := bytes.NewBuffer(b)
	val, vtype, err := ReadValue(buf)
	if err != nil { return nil, "", err }
	return val, vtype, nil
}

func (s *Redis) Start() error {
	return nil
}

func (s *Redis) Stop() error {
	return nil
}

func (s *Redis) ExecuteRead(cmd string, key string, args []string) (store.Value, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	switch cmd {
	case GET:
		//
		if err := s.validateGet(key, args); err != nil { return nil, err }
		rval, err := s.get(key)
		if err != nil { return nil, err }
		return rval, nil
	default:
		return nil, fmt.Errorf("Unrecognized read command: %v", cmd)
	}

	return nil, nil
}

func (s *Redis) ExecuteWrite(cmd string, key string, args []string, timestamp time.Time) (store.Value, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	switch cmd {
	case SET:
		if err := s.validateSet(key, args, timestamp); err != nil { return nil, err }
		return s.set(key, args[0], timestamp), nil
	case DEL:
		if err := s.validateDel(key, args, timestamp); err != nil { return nil, err }
		return s.del(key, timestamp)
	default:
		return nil, fmt.Errorf("Unrecognized read command: %v", cmd)
	}
	return nil, nil
}

// reconciles multiple values and returns instructions for correcting
// the values on inaccurate nodes
//
// Reconcile should handle value maps with one value without hitting
// a value type specific reconciliation function
//
// value type specific reconciliation functions should be able to handle
// getting unfamiliar types, but can operate under the assumption that if
// they're being called, the oldest timestamp of the given values belongs
// to a value of it's type.
func (s *Redis) Reconcile(key string, values map[string] store.Value) (store.Value, map[string][]*store.Instruction, error) {
	switch len(values){
	case 0:
		return nil, nil, fmt.Errorf("At least one value must be provided")
	case 1:
		var val store.Value
		for _, v := range values { val = v }
		return val, nil, nil
	default:
		// check for inconsistencies
		// and record the highest timestamp and it's value type
		var last store.Value
		var highTimeStamp time.Time
		var highValueType store.ValueType
		count := 0
		reconcile := false
		for _, val := range values {
			if count > 0 { reconcile = reconcile || val != last }
			last = val
			count++

			if ts := val.GetTimestamp(); ts.After(highTimeStamp) {
				highTimeStamp = ts
				highValueType = val.GetValueType()
			}
		}

		if reconcile {
			switch highValueType {
			case STRING_VALUE:
				//
				return reconcileString(key, values)
			case TOMBSTONE_VALUE:
				//
			default:
				return nil, nil, fmt.Errorf("Unknown value type: %v", highValueType)

			}
		}
	}
	return nil, make(map[string][]*store.Instruction), nil
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

// ----------- data import / export -----------


// blindly gets the contents of the given key
func (s *Redis) GetRawKey(key string) (store.Value, error) {
	val, ok := s.data[key]
	if !ok {
		return nil, fmt.Errorf("key [%v] does not exist", key)
	}
	return val, nil
}

// blindly sets the contents of the given key
func (s *Redis) SetRawKey(key string, val store.Value) error {
	s.data[key] = val
	return nil
}

// returns all of the keys held by the store, including keys containing
// tombstones
func (s *Redis) GetKeys() []string {
	var i int
	keys := make([]string, len(s.data))
	for key, _ := range s.data {
		keys[i] = key
		i ++
	}
	return keys
}
