package values

import (
	"testing"
	"time"

	"redis"
	"testing_helpers"
	"store"
)

func TestTombstoneValue(t *testing.T) {
	s := redis.NewDefaultRedis()
	src := NewTombstone(time.Now())

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
	dst, ok := val.(*Tombstone)
	if !ok {
		t.Fatalf("Unexpected value type: %T", val)
	}

	testing_helpers.AssertEqual(t, "time", src.time, dst.time)
}

// tests that the tombstone struct satisfies the
// value interface
func TestTombstoneInterface(t *testing.T) {
	func (store.Value){}(NewTombstone(time.Now()))
}
