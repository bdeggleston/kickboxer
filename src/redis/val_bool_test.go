package redis

import (
	"testing"
	"time"

	"testing_helpers"
)

func TestBooleanValue(t *testing.T) {
	s := setupRedis()
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
