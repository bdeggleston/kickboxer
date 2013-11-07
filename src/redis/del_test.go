package redis

import (
	"testing"
	"testing_helpers"
	"time"
	"store"
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
	ts := time.Now()
	rawval, err := r.ExecuteWrite("DEL", "a", []string{}, ts)
	if err != nil {
		t.Fatalf("Unexpected error deleting 'a': %v", err)
	}
	val, ok := rawval.(*boolValue)
	if !ok {
		t.Fatalf("Unexpected value type: %T", val)
	}

	testing_helpers.AssertEqual(t, "value", val.value, true)
	testing_helpers.AssertEqual(t, "time", val.time, expected.time)

	// check tombstone
	rawval, exists = r.data["a"]
	if !exists {
		t.Fatalf("Expected tombstone, got nil")
	}
	tsval, ok := rawval.(*tombstoneValue)
	if !ok {
		t.Errorf("tombstone value of unexpected type: %T", rawval)
	}
	testing_helpers.AssertEqual(t, "time", tsval.time, ts)
}

func TestDelNonExistingVal(t *testing.T) {
	r := setupRedis()

	// sanity check
	_, exists := r.data["a"]
	if exists {
		t.Errorf("Value unexpectedly found for 'a'")
	}

	// delete value
	ts := time.Now()
	rawval, err := r.ExecuteWrite("DEL", "a", []string{}, ts)
	if err != nil {
		t.Fatalf("Unexpected error deleting 'a': %v", err)
	}
	val, ok := rawval.(*boolValue)
	if !ok {
		t.Fatalf("Unexpected value type: %T", val)
	}

	testing_helpers.AssertEqual(t, "value", val.value, false)
	testing_helpers.AssertEqual(t, "time", val.time, time.Time{})

	// check tombstone
	rawval, exists = r.data["a"]
	if exists {
		t.Fatalf("Unexpected tombstone val found: %T %v", rawval, rawval)
	}
}

// tests validation of DEL insructions
func TestDelValidation(t *testing.T) {
	r := setupRedis()

	var val store.Value
	var err error

	val, err = r.ExecuteWrite("DEL", "a", []string{"x", "y"}, time.Now())
	if val != nil { t.Errorf("Expected nil value, got %v", val) }
	if err == nil {
		t.Errorf("Expected error, got nil")
	} else {
		t.Logf("Got expected err: %v", err)
	}

	val, err = r.ExecuteWrite("DEL", "a", []string{}, time.Time{})
	if val != nil { t.Errorf("Expected nil value, got %v", val) }
	if err == nil {
		t.Errorf("Expected error, got nil")
	} else {
		t.Logf("Got expected err: %v", err)
	}
}
