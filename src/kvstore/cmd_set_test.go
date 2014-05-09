package kvstore

import (
	"testing"
	"testing_helpers"
	"time"

	"store"
)

// tests basic function of set
func TestSet(t *testing.T) {
	r := setupKVStore()

	// sanity check
	_, exists := r.data["a"]
	if exists {
		t.Fatalf("Unexpectedly found 'a' in store")
	}

	ts := time.Now()
	rawval, err := r.ExecuteQuery("SET", "a", []string{"b"}, ts)
	if err != nil {
		t.Errorf("Unexpected write error: %v", err)
	}
	rval, ok := rawval.(*String)
	if !ok {
		t.Errorf("returned value of unexpected type: %T", rawval)
	}

	actualraw, exists := r.data["a"]
	if ! exists {
		t.Errorf("No value found for 'a'")
	}
	actual, ok := actualraw.(*String)
	if !ok {
		t.Errorf("actual value of unexpected type: %T", actualraw)
	}

	testing_helpers.AssertEqual(t, "rval data", "b", rval.GetValue())
	testing_helpers.AssertEqual(t, "actual data", "b", actual.GetValue())
	testing_helpers.AssertEqual(t, "rval time", ts, rval.GetTimestamp())
	testing_helpers.AssertEqual(t, "actual time", ts, actual.GetTimestamp())
}

// if set is called with a timestamp which is lower than
// the existing value, it should be ignored
func TestSetConflictingTimestamp(t *testing.T) {
	r := setupKVStore()
	now := time.Now()
	then := now.Add(time.Duration(-1))
	expected := r.set("a", "b", now)
	actual := r.set("a", "c", then)
	testing_helpers.AssertEqual(t, "set val", expected, actual)

}

// tests validation of SET insructions
func TestSetValidation(t *testing.T) {
	r := setupKVStore()

	var val store.Value
	var err error

	val, err = r.ExecuteQuery("SET", "a", []string{"x", "y"}, time.Now())
	if val != nil { t.Errorf("Expected nil value, got %v", val) }
	if err == nil {
		t.Errorf("Expected error, got nil")
	} else {
		t.Logf("Got expected err: %v", err)
	}

	val, err = r.ExecuteQuery("SET", "a", []string{"x"}, time.Time{})
	if val != nil { t.Errorf("Expected nil value, got %v", val) }
	if err == nil {
		t.Errorf("Expected error, got nil")
	} else {
		t.Logf("Got expected err: %v", err)
	}
}

