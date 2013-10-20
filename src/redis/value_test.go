package redis

import (
	"fmt"
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

// tests the single value
func TestSingleValue(t *testing.T) {
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

func TestDelResponseValue(t *testing.T) {
	s := &Redis{}
	src := &delResponseValue{keys:[]string{"a", "b", "c"}}

	b, err := s.SerializeValue(src)
	if err != nil {
		t.Fatalf("Unexpected serialization error: %v", err)
	}

	val, vtype, err := s.DeserializeValue(b)
	if err != nil {
		t.Fatalf("Unexpected deserialization error: %v", err)
	}
	if vtype != DEL_RESPONSE_VALUE {
		t.Fatalf("Unexpected value type enum: %v", vtype)
	}
	dst, ok := val.(*delResponseValue)
	if !ok {
		t.Fatalf("Unexpected value type: %T", val)
	}

	testing_helpers.AssertEqual(t, "key len", len(src.keys), len(dst.keys))

	for i:=0; i<len(src.keys); i++ {
		testing_helpers.AssertEqual(t, fmt.Sprintf("key[%v]", i), src.keys[i], dst.keys[i])
	}
}

