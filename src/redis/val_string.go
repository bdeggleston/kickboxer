package redis

import (
	"bufio"
	"time"

	"store"
	"serializer"
)

// a single value used for
// key/val types
type String struct {
	value string
	time time.Time
}

// single value constructor
func NewString(value string, time time.Time) *String {
	v := &String{
		value:value,
		time:time,
	}
	return v
}

func (v *String) GetValue() string {
	return v.value
}

func (v *String) GetTimestamp() time.Time {
	return v.time
}

func (v *String) GetValueType() store.ValueType {
	return STRING_VALUE
}

func (v *String) Equal(o store.Value) bool {
	if !baseValueEqual(v, o) { return false }
	other := o.(*String)
	if v.value != other.value { return false }
	return true
}

func (v *String) Serialize(buf *bufio.Writer) error {
	if err := serializer.WriteFieldBytes(buf, []byte(v.value)); err != nil {
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

func (v *String) Deserialize(buf *bufio.Reader) error {
	if val, err := serializer.ReadFieldBytes(buf); err != nil {
		return err
	} else {
		v.value = string(val)
	}

	if t, err := serializer.ReadTime(buf); err != nil {
		return err
	} else {
		v.time = t
	}
	return nil
}

func reconcileString(key string, highValue *String, values map[string]store.Value) (*String, map[string][]*store.Instruction, error) {
	// create instructions for the unequal nodes
	instructions := make(map[string][]*store.Instruction)
	for nodeid, val := range values {
		// TODO: use Value.Equal() method
		if val != highValue {
			instructions[nodeid] = []*store.Instruction{&store.Instruction{
				Cmd:"SET",
				Key:key,
				Args:[]string{highValue.value},
				Timestamp:highValue.time,
			}}
		}
	}

	return highValue, instructions, nil
}
