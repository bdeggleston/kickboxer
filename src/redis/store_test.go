package redis

import (
	"testing"
	"time"
	"testing_helpers"
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

func TestInterfaceIsImplemented(t *testing.T) {
	t.Skipf("Not working yet!")
//	tt := func(s store.Store) {}
//	tt(&Redis{})
}

/***************** query tests *****************/

func setupRedis() *Redis {
	r := NewRedis()
	return r
}

// ----------- data import / export -----------

func TestGetRawKeySuccess(t *testing.T) {
	r := setupRedis()
	expected, err := r.ExecuteWrite("SET", "a", []string{"b"}, time.Now())
	if err != nil {
		t.Fatalf("Unexpected error executing set: %v", err)
	}

	rval, err := r.GetRawKey("a")
	if err != nil {
		t.Fatal("unexpectedly got error: %v", err)
	}
	val, ok := rval.(*stringValue)
	if !ok {
		t.Fatal("expected value of type stringValue, got %T", rval)
	}
	testing_helpers.AssertEqual(t, "value", expected, val)
}

func TestSetRawKey(t *testing.T) {
	r := setupRedis()
	expected := newBoolValue(true, time.Now())
	r.SetRawKey("x", expected)

	rval, exists := r.data["x"]
	if !exists {
		t.Fatalf("no value found for key 'x'")
	}
	val, ok := rval.(*boolValue)
	if !ok {
		t.Fatal("expected value of type boolValues, got %T", rval)
	}

	testing_helpers.AssertEqual(t, "val", expected, val)
}

func TestGetKeys(t *testing.T) {

}
