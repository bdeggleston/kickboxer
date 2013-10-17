package store

import (
	"bytes"
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

// compile time assertion that Value is implemented
func valueInterfaceCheck(v Value) {}

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

/***************** value tests *****************/

// tests the single value
func TestSingleValue(t *testing.T) {
	buf := &bytes.Buffer{}
	src := newSingleValue("blake", time.Now())
	valueInterfaceCheck(src)

	if err := WriteRedisValue(buf, src); err != nil {
		t.Fatalf("Unexpected serialization error: %v", err)
	}

	val, vtype, err := ReadRedisValue(buf)
	if err != nil {
		t.Fatalf("Unexpected deserialization error: %v", err)
	}
	if vtype != SINGLE_VALUE {
		t.Fatalf("Unexpected value type enum: %v", vtype)
	}
	dst, ok := val.(*singleValue)
	if !ok {
		t.Fatalf("Unexpected value type: %T", val)
	}

	testing_helpers.AssertEqual(t, "data", src.data, dst.data)
	testing_helpers.AssertEqual(t, "time", src.time, dst.time)
}
