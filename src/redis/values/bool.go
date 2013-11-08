package values

import (
	"bufio"
	"time"

	"serializer"
	"store"
)

type Boolean struct {
	value bool
	time time.Time
}

func NewBoolean(val bool, timestamp time.Time) *Boolean {
	v := &Boolean{
		value:val,
		time:timestamp,
	}
	return v
}

func (v *Boolean) GetValue() bool {
	return v.value
}

func (v *Boolean) GetTimestamp() time.Time {
	return v.time
}

func (v *Boolean) GetValueType() store.ValueType {
	return BOOL_VALUE
}

func (v *Boolean) Serialize(buf *bufio.Writer) error {
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

func (v *Boolean) Deserialize(buf *bufio.Reader) error {
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
