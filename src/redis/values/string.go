package values

import (
	"bufio"
	"time"

	"store"
	"serializer"
)

// a single value used for
// key/val types
type String struct {
	data string
	time time.Time
}

// single value constructor
func NewString(data string, time time.Time) *String {
	v := &String{
		data:data,
		time:time,
	}
	return v
}

func (v *String) GetTimestamp() time.Time {
	return v.time
}

func (v *String) GetValueType() store.ValueType {
	return STRING_VALUE
}

func (v *String) Serialize(buf *bufio.Writer) error {
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

func (v *String) Deserialize(buf *bufio.Reader) error {
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
