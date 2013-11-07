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
	STRING_VALUE = store.ValueType("STRING")
	TOMBSTONE_VALUE	= store.ValueType("TOMBSTONE")
	BOOL_VALUE	= store.ValueType("BOOL")
)

// a single value used for
// key/val types
type stringValue struct {
	data string
	time time.Time
}

// single value constructor
func newStringValue(data string, time time.Time) *stringValue {
	v := &stringValue{
		data:data,
		time:time,
	}
	return v
}

func (v *stringValue) GetTimestamp() time.Time {
	return v.time
}

func (v *stringValue) GetValueType() store.ValueType {
	return STRING_VALUE
}

func (v *stringValue) Serialize(buf *bufio.Writer) error {
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

func (v *stringValue) Deserialize(buf *bufio.Reader) error {
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

type boolValue struct {
	value bool
	time time.Time
}

func newBoolValue(val bool, timestamp time.Time) *boolValue {
	v := &boolValue{
		value:val,
		time:timestamp,
	}
	return v
}

func (v *boolValue) GetTimestamp() time.Time {
	return v.time
}

func (v *boolValue) GetValueType() store.ValueType {
	return BOOL_VALUE
}

func (v *boolValue) Serialize(buf *bufio.Writer) error {
	var b byte
	if v.value { b = 0xff }
	if err := buf.WriteByte(b); err != nil {
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

func (v *boolValue) Deserialize(buf *bufio.Reader) error {
	if b, err := buf.ReadByte(); err != nil {
		return err
	} else {
		v.value = b != 0x00
	}
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
	case STRING_VALUE:
		value = &stringValue{}
	case TOMBSTONE_VALUE:
		value = &tombstoneValue{}
	case BOOL_VALUE:
		value = &boolValue{}
	default:
		return nil, "", fmt.Errorf("Unexpected value type: %v", vtype)
	}

	if err := value.Deserialize(reader); err != nil { return nil, "", err}
	return value, vtype, nil
}

