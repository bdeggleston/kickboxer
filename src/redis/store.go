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
	if err := WriteRedisValue(buf, v) ; err != nil { return nil, err }
	return buf.Bytes(), nil
}

func (s *Redis) DeserializeValue(b []byte) (store.Value, store.ValueType, error) {
	buf := bytes.NewBuffer(b)
	val, vtype, err := ReadRedisValue(buf)
	if err != nil { return nil, "", err }
	return val, vtype, nil
}

func (s *Redis) Start() error {
	return nil
}

func (s *Redis) Stop() error {
	return nil
}

// Get the value of key. If the key does not exist the special value nil is returned.
// An error is returned if the value stored at key is not a string, because GET only handles string values.
func (s *Redis) get(key string) (store.Value, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	// TODO: check that the returned value is a string or tombstone value
	return s.data[key], nil
}

func (s *Redis) ExecuteRead(cmd string, key string, args []string) (store.Value, error) {
	switch cmd {
	case GET:
		//
		if len(args) != 0 { return nil, fmt.Errorf("too many args for GET") }
		rval, err := s.get(key)
		if err != nil { return nil, err }
		return rval, nil
	default:
		return nil, fmt.Errorf("Unrecognized read command: %v", cmd)
	}

	return nil, nil
}

// Set key to hold the string value. If key already holds a value, it is overwritten,
// regardless of its type. Any previous time to live associated with the key is discarded
// on successful SET operation.
func (s *Redis) set(key string, val string, ts time.Time) (store.Value) {
	existing, exists := s.data[key]
	if exists && ts.Before(existing.GetTimestamp()) {
		return existing
	}
	value := newStringValue(val, ts)
	s.data[key] = value
	return value
}

// Removes the specified keys. A key is ignored if it does not exist.
// Return value: Integer reply: The number of keys that were removed.
//
// returns a list of deleted keys to be reconciled by the coordinator
// to return the number of deleted keys
func (s *Redis) del(keys []string, ts time.Time) {
	deleted := make([]string, 0, len(keys))
	for _, key := range keys {
		if _, ok := s.data[key]; ok {
			s.data[key] = newTombstoneValue(ts)
			deleted = append(deleted, key)
		}
	}
}

func (s *Redis) ExecuteWrite(cmd string, key string, args []string, timestamp time.Time) (store.Value, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	switch cmd {
	case SET:
		//
		if len(args) != 1 {
			return nil, fmt.Errorf("incorrect number of args for SET. Expected 1, got %v", len(args))
		}
		return s.set(key, args[0], timestamp), nil
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
