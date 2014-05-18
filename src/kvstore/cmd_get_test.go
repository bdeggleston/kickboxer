package kvstore

import (
	"store"
	"testing"
	"time"
	"testing_helpers"
)

func TestGet(t *testing.T) {
	r := setupKVStore()
	expected := NewString("b", time.Now())
	r.data["a"] = expected

	val, err := r.ExecuteInstruction(store.NewInstruction("GET", "a", []string{}, time.Time{}))
	if err != nil {
		t.Fatalf("Unexpected error on read: %v", err)
	}
	actual, ok := val.(*String)
	if !ok {
		t.Fatalf("Unexpected value type: %T", val)
	}

	testing_helpers.AssertEqual(t, "data", expected.GetValue(), actual.GetValue())
	testing_helpers.AssertEqual(t, "time", expected.GetTimestamp(), actual.GetTimestamp())
}

// tests that calling get on a key holding a value other than
// a string value returns an error
func TestGetNonStringFails(t *testing.T) {
	t.Skipf("other types not implemented yet")
}

// tests validation of GET insructions
func TestGetValidation(t *testing.T) {
	r := setupKVStore()

	// too many args
	val, err := r.ExecuteInstruction(store.NewInstruction("GET", "a", []string{"b"}, time.Time{}))
	if val != nil {
		t.Errorf("Unexpected non-nil value")
	}
	if err == nil {
		t.Errorf("Expected error, got nil")
	} else {
		t.Logf("Got expected err: %v", err)
	}
}

