package redis

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

import (
	"serializer"
	"store"
)

const (
	SINGLE_VALUE = store.ValueType("SINGLE")
	TOMBSTONE_VALUE	= store.ValueType("TOMBSTONE")
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

func (v *singleValue) GetValueType() store.ValueType {
	return SINGLE_VALUE
}

func (v *singleValue) Serialize(buf *bufio.Writer) error {
	if err := serializer.WriteFieldBytes(buf, []byte(v.data)); err != nil {
		return err
	}
	if err := serializer.WriteTime(buf, v.time); err != nil {
		return err
	}
	if err := buf.Flush(); err != nil {
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

func WriteRedisValue(buf io.Writer, v store.Value) error {
	writer := bufio.NewWriter(buf)

	vtype := v.GetValueType()
	if err := serializer.WriteFieldBytes(writer, []byte(vtype)); err != nil { return err }
	if err := v.Serialize(writer); err != nil { return err }
	if err := writer.Flush(); err != nil { return err }
	return nil
}

func ReadRedisValue(buf io.Reader) (store.Value, store.ValueType, error) {
	reader := bufio.NewReader(buf)
	vstr, err := serializer.ReadFieldBytes(reader)
	if err != nil { return nil, "", err }

	vtype := store.ValueType(vstr)
	var value store.Value
	switch vtype {
	case SINGLE_VALUE:
		value = &singleValue{}
	default:
		return nil, "", fmt.Errorf("Unexpected value type: %v", vtype)
	}

	if err := value.Deserialize(reader); err != nil { return nil, "", err}
	return value, vtype, nil
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

// returns the contents of the given key
func (s *Redis) get(key string) store.Value {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.data[key]
}

func (s *Redis) ExecuteRead(cmd string, key string, args []string) (store.Value, error) {
	switch cmd {
	case GET:
		//
		if len(args) != 0 { return nil, fmt.Errorf("too many args for GET") }
		return s.get(key), nil
	default:
		return nil, fmt.Errorf("Unrecognized read command: %v", cmd)
	}

	return nil, nil
}

func (s *Redis) set(key string, val string, ts time.Time) (store.Value) {
	existing, exists := s.data[key]
	if exists && ts.Before(existing.GetTimestamp()) {
		return existing
	}
	value := newSingleValue(val, ts)
	s.data[key] = value
	return value
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
