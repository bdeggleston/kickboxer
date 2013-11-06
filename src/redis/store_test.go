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
