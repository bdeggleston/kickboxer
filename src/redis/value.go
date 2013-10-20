package redis

import (
	"bufio"
	"fmt"
	"io"
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

// a value indicating a deletion
type tombstoneValue struct {
	time time.Time
}

// single value constructor
func newTombstoneValue(time time.Time) *tombstoneValue {
	v := &tombstoneValue{
		time:time,
	}
	return v
}

func (v *tombstoneValue) GetTimestamp() time.Time {
	return v.time
}

func (v *tombstoneValue) GetValueType() store.ValueType {
	return TOMBSTONE_VALUE
}

func (v *tombstoneValue) Serialize(buf *bufio.Writer) error {
	if err := serializer.WriteTime(buf, v.time); err != nil {
		return err
	}
	if err := buf.Flush(); err != nil {
		return err
	}
	return nil
}

func (v *tombstoneValue) Deserialize(buf *bufio.Reader) error {
	if t, err := serializer.ReadTime(buf); err != nil {
		return err
	} else {
		v.time = t
	}
	return nil
}

/***************** reader/writer functions *****************/

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
	case TOMBSTONE_VALUE:
		value = &tombstoneValue{}
	default:
		return nil, "", fmt.Errorf("Unexpected value type: %v", vtype)
	}

	if err := value.Deserialize(reader); err != nil { return nil, "", err}
	return value, vtype, nil
}

