package redis

import (
	"testing"
	"time"

	"store"
	"testing_helpers"
)

// test that a tombstone value is written
func TestDelExistingVal(t *testing.T) {
	r := NewDefaultRedis()

	// write value
	if _, err := r.ExecuteWrite("SET", "a", []string{"b"}, time.Now()); err != nil {
		t.Fatalf("Unexpected error setting 'a': %v", err)
	}

	// sanity check
	oldval, exists := r.data["a"]
	if ! exists {
		t.Errorf("No value found for 'a'")
	}
	expected, ok := oldval.(*String)
	if !ok {
		t.Errorf("actual value of unexpected type: %T", oldval)
	}

	// delete value
	ts := time.Now()
	rawval, err := r.ExecuteWrite("DEL", "a", []string{}, ts)
	if err != nil {
		t.Fatalf("Unexpected error deleting 'a': %v", err)
	}
	val, ok := rawval.(*Boolean)
	if !ok {
		t.Fatalf("Unexpected value type: %T", val)
	}

	testing_helpers.AssertEqual(t, "value", val.GetValue(), true)
	testing_helpers.AssertEqual(t, "time", val.GetTimestamp(), expected.GetTimestamp())

	// check tombstone
	rawval, exists = r.data["a"]
	if !exists {
		t.Fatalf("Expected tombstone, got nil")
	}
	tsval, ok := rawval.(*Tombstone)
	if !ok {
		t.Errorf("tombstone value of unexpected type: %T", rawval)
	}
	testing_helpers.AssertEqual(t, "time", tsval.GetTimestamp(), ts)
}

func TestDelNonExistingVal(t *testing.T) {
	r := NewDefaultRedis()

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
	val, ok := rawval.(*Boolean)
	if !ok {
		t.Fatalf("Unexpected value type: %T", val)
	}

	testing_helpers.AssertEqual(t, "value", val.GetValue(), false)
	testing_helpers.AssertEqual(t, "time", val.GetTimestamp(), time.Time{})

	// check tombstone
	rawval, exists = r.data["a"]
	if exists {
		t.Fatalf("Unexpected tombstone val found: %T %v", rawval, rawval)
	}
}

// tests validation of DEL insructions
func TestDelValidation(t *testing.T) {
	r := NewDefaultRedis()

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
