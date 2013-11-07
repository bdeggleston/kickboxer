package redis

import (
	"testing"
	"time"
	"testing_helpers"
)

func TestGet(t *testing.T) {
	r := setupRedis()
	expected := newStringValue("b", time.Now())
	r.data["a"] = expected

	val, err := r.ExecuteRead("GET", "a", []string{})
	if err != nil {
		t.Fatalf("Unexpected error on read: %v", err)
	}
	actual, ok := val.(*stringValue)
	if !ok {
		t.Fatalf("Unexpected value type: %T", val)
	}

	testing_helpers.AssertEqual(t, "data", expected.data, actual.data)
	testing_helpers.AssertEqual(t, "time", expected.time, actual.time)
}

// tests that calling get on a key holding a value other than
// a string value returns an error
func TestGetNonStringFails(t *testing.T) {
	t.Skipf("other types not implemented yet")
}

// tests validation of GET insructions
func TestGetValidation(t *testing.T) {
	r := setupRedis()

	// too many args
	val, err := r.ExecuteRead("GET", "a", []string{"b"})
	if val != nil {
		t.Errorf("Unexpected non-nil value")
	}
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
}

