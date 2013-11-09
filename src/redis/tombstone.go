package redis

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

