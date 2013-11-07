package redis

import (
	"testing"
	"testing_helpers"
	"time"
)

// test that a tombstone value is written
func TestDelExistingVal(t *testing.T) {
	r := setupRedis()

	// write value
	if _, err := r.ExecuteWrite("SET", "a", []string{"b"}, time.Now()); err != nil {
		t.Fatalf("Unexpected error setting 'a': %v", err)
	}

	// sanity check
	oldval, exists := r.data["a"]
	if ! exists {
		t.Errorf("No value found for 'a'")
	}
	expected, ok := oldval.(*stringValue)
	if !ok {
		t.Errorf("actual value of unexpected type: %T", oldval)
	}

	// delete value
	rawval, err := r.ExecuteWrite("DEL", "a", []string{}, time.Now())
	if err != nil {
		t.Fatalf("Unexpected error deleting 'a': %v", err)
	}
	val, ok := rawval.(*boolValue)
	if !ok {
		t.Fatalf("Unexpected value type: %T", val)
	}

	testing_helpers.AssertEqual(t, "value", val.value, true)
	testing_helpers.AssertEqual(t, "time", val.time, expected.time)
}

func TestDelNonExistingVal(t *testing.T) {
	r := setupRedis()

	// sanity check
	_, exists := r.data["a"]
	if exists {
		t.Errorf("Value unexpectedly found for 'a'")
	}

	// delete value
	rawval, err := r.ExecuteWrite("DEL", "a", []string{}, time.Now())
	if err != nil {
		t.Fatalf("Unexpected error deleting 'a': %v", err)
	}
	val, ok := rawval.(*boolValue)
	if !ok {
		t.Fatalf("Unexpected value type: %T", val)
	}

	testing_helpers.AssertEqual(t, "value", val.value, false)
	testing_helpers.AssertEqual(t, "time", val.time, time.Time{})
}

// tests validation of DEL insructions
func TestDelValidation(t *testing.T) {
	t.Skip("no validation at the moment")
}
