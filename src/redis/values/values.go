package values

import (
	"bufio"
	"fmt"
	"io"

	"serializer"
	"store"
)

const (
	STRING_VALUE = store.ValueType("STRING")
	TOMBSTONE_VALUE	= store.ValueType("TOMBSTONE")
	BOOL_VALUE	= store.ValueType("BOOL")
)

func WriteValue(buf io.Writer, v store.Value) error {
	writer := bufio.NewWriter(buf)

	vtype := v.GetValueType()
	if err := serializer.WriteFieldBytes(writer, []byte(vtype)); err != nil { return err }
	if err := v.Serialize(writer); err != nil { return err }
	if err := writer.Flush(); err != nil { return err }
	return nil
}

func ReadValue(buf io.Reader) (store.Value, store.ValueType, error) {
	reader := bufio.NewReader(buf)
	vstr, err := serializer.ReadFieldBytes(reader)
	if err != nil { return nil, "", err }

	vtype := store.ValueType(vstr)
	var value store.Value
	switch vtype {
	case STRING_VALUE:
		value = &String{}
	case TOMBSTONE_VALUE:
		value = &Tombstone{}
	case BOOL_VALUE:
		value = &Boolean{}
	default:
		return nil, "", fmt.Errorf("Unexpected value type: %v", vtype)
	}

	if err := value.Deserialize(reader); err != nil { return nil, "", err}
	return value, vtype, nil
}
