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
		val, err := s.del(key, timestamp)
		if err != nil { return nil, err }
		return val, nil
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