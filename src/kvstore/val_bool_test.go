package kvstore

import (
	"testing"
	"time"

	"testing_helpers"
)

func TestBooleanValue(t *testing.T) {
	s := setupKVStore()
	for _, val := range []bool{true, false} {
		src := NewBoolean(val, time.Now())

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
		dst, ok := val.(*Boolean)
		if !ok {
			t.Fatalf("Unexpected value type: %T", val)
		}

		testing_helpers.AssertEqual(t, "value", src.value, dst.value)
		testing_helpers.AssertEqual(t, "time", src.time, dst.time)
	}
}

// tests the boolean value's equality method
func TestBooleanEquality(t *testing.T) {
	t0 := time.Now()
	v0 := NewBoolean(true, t0)

	testing_helpers.AssertEqual(t, "equal value", true, v0.Equal(NewBoolean(true, t0)))
	testing_helpers.AssertEqual(t, "unequal timestamp", false, v0.Equal(NewBoolean(true, t0.Add(4))))
	testing_helpers.AssertEqual(t, "unequal value", false, v0.Equal(NewBoolean(false, t0)))
	testing_helpers.AssertEqual(t, "unequal type", false, v0.Equal(NewString("asdf", t0)))
}
