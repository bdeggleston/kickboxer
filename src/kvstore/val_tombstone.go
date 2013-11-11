package kvstore

import (
	"bufio"
	"time"

	"serializer"
	"store"
)

// a value indicating a deletion
type Tombstone struct {
	time time.Time
}

// single value constructor
func NewTombstone(time time.Time) *Tombstone {
	v := &Tombstone{
		time:time,
	}
	return v
}

func (v *Tombstone) GetTimestamp() time.Time {
	return v.time
}

func (v *Tombstone) GetValueType() store.ValueType {
	return TOMBSTONE_VALUE
}

func (v *Tombstone) Equal(o store.Value) bool {
	return baseValueEqual(v, o)
}

func (v *Tombstone) Serialize(buf *bufio.Writer) error {
	if err := serializer.WriteTime(buf, v.time); err != nil {
		return err
	}
	if err := buf.Flush(); err != nil {
		return err
	}
	return nil
}

func (v *Tombstone) Deserialize(buf *bufio.Reader) error {
	if t, err := serializer.ReadTime(buf); err != nil {
		return err
	} else {
		v.time = t
	}
	return nil
}

func reconcileTombstone(key string, highValue *Tombstone, values map[string]store.Value) (*Tombstone, map[string][]*store.Instruction, error) {
	// create instructions for the unequal nodes
	instructions := make(map[string][]*store.Instruction)
	for nodeid, val := range values {
		if val != highValue {
			instructions[nodeid] = []*store.Instruction{&store.Instruction{
				Cmd:"DEL",
				Key:key,
				Args:[]string{},
				Timestamp:highValue.time,
			}}
		}
	}

	return highValue, instructions, nil
}
