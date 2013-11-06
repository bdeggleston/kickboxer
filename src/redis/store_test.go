package redis

import (
	"testing"
	"testing_helpers"
	"time"
)

// table of instructions, and whether they're
// a write (true) or read (false)
var isWrite = []struct {
	cmd string
	result bool
}{
	{"GET", false},
	{"SET", true},
	{"DEL", true},
}

func TestIsWriteCmd(t *testing.T) {
	r := &Redis{}
	for _, c := range isWrite {
		if result := r.IsWriteCommand(c.cmd); result != c.result {
			if result {
				t.Errorf("%v erroneously identified as a write", c.cmd)
			} else {
				t.Errorf("%v not identified as a write", c.cmd)
			}
		}
	}
}

func TestIsReadCmd(t *testing.T) {
	r := &Redis{}
	for _, c := range isWrite {
		if result := r.IsReadCommand(c.cmd); result != !c.result {
			if result {
				t.Errorf("%v erroneously identified as a read", c.cmd)
			} else {
				t.Errorf("%v not identified as a read", c.cmd)
			}
		}
	}
}
/***************** query tests *****************/

func setupRedis() *Redis {
	r := NewRedis()
	return r
}

// tests basic function of set
func TestSet(t *testing.T) {
	r := setupRedis()

	// sanity check
	_, exists := r.data["a"]
	if exists {
		t.Fatalf("Unexpectedly found 'a' in store")
	}

	ts := time.Now()
	rawval, err := r.ExecuteWrite("SET", "a", []string{"b"}, ts)
	if err != nil {
		t.Errorf("Unexpected write error: %v", err)
	}
	rval, ok := rawval.(*stringValue)
	if !ok {
		t.Errorf("returned value of unexpected type: %T", rawval)
	}

	actualraw, exists := r.data["a"]
	if ! exists {
		t.Errorf("No value found for 'a'")
	}
	actual, ok := actualraw.(*stringValue)
	if !ok {
		t.Errorf("actual value of unexpected type: %T", actualraw)
	}

	testing_helpers.AssertEqual(t, "rval data", "b", rval.data)
	testing_helpers.AssertEqual(t, "actual data", "b", actual.data)
	testing_helpers.AssertEqual(t, "rval time", ts, rval.time)
	testing_helpers.AssertEqual(t, "actual time", ts, actual.time)
}

// if set is called with a timestamp which is lower than
// the existing value, it should be ignored
func TestSetConflictingTimestamp(t *testing.T) {

}

// tests validation of SET insructions
func TestSetValidation(t *testing.T) {
	r := setupRedis()
	val, err := r.ExecuteWrite("SET", "a", []string{"x", "y"}, time.Now())
	if val != nil {
		t.Errorf("Expected nil value, got %v", val)
	}
	if err == nil {
		t.Errorf("Expected error, got nil")
	}

}

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

func TestDel(t *testing.T) {

}

// tests that the correct number of deleted keys are returned
func TestDelReturnVal(t *testing.T) {

}

// tests validation of DEL insructions
func TestDelValidation(t *testing.T) {

}
