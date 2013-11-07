package redis

import (
	"testing"
	"time"
)

import (
	"store"
	"testing_helpers"
)

// compile time assertion that Value is implemented
func valueInterfaceCheck(_ store.Value) {}

/***************** value tests *****************/

// tests the string value
func TestStringValue(t *testing.T) {
	s := &Redis{}
	src := newStringValue("blake", time.Now())
	valueInterfaceCheck(src)

	b, err := s.SerializeValue(src)
	if err != nil {
		t.Fatalf("Unexpected serialization error: %v", err)
	}

	val, vtype, err := s.DeserializeValue(b)
	if err != nil {
		t.Fatalf("Unexpected deserialization error: %v", err)
	}
	if vtype != STRING_VALUE {
		t.Fatalf("Unexpected value type enum: %v", vtype)
	}
	dst, ok := val.(*stringValue)
	if !ok {
		t.Fatalf("Unexpected value type: %T", val)
	}

	testing_helpers.AssertEqual(t, "data", src.data, dst.data)
	testing_helpers.AssertEqual(t, "time", src.time, dst.time)
}

func TestBoolValue(t *testing.T) {
	s := &Redis{}
	for _, val := range []bool{true, false} {
		src := newBoolValue(val, time.Now())

		b, err := s.SerializeValue(src)
		if err != nil {
			t.Fatalf("Unexpected serialization error: %v", err)
		}

		val, vtype, err := s.DeserializeValue(b)
		if err != nil {
			t.Fatalf("Unexpected deserialization error: %v", err)
		}
		if vtype != BOOL_VALUE {
			t.Fatalf("Unexpected value type enum: %v", vtype)
		}
		dst, ok := val.(*boolValue)
		if !ok {
			t.Fatalf("Unexpected value type: %T", val)
		}

		testing_helpers.AssertEqual(t, "value", src.value, dst.value)
		testing_helpers.AssertEqual(t, "time", src.time, dst.time)
	}
}

func TestTombstoneValue(t *testing.T) {
	s := &Redis{}
	src := newTombstoneValue(time.Now())

	b, err := s.SerializeValue(src)
	if err != nil {
		t.Fatalf("Unexpected serialization error: %v", err)
	}

	val, vtype, err := s.DeserializeValue(b)
	if err != nil {
		t.Fatalf("Unexpected deserialization error: %v", err)
	}
	if vtype != TOMBSTONE_VALUE {
		t.Fatalf("Unexpected value type enum: %v", vtype)
	}
	dst, ok := val.(*tombstoneValue)
	if !ok {
		t.Fatalf("Unexpected value type: %T", val)
	}

	testing_helpers.AssertEqual(t, "time", src.time, dst.time)
}

