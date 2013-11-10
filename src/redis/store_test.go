package redis

import (
	"testing"
	"time"

	"testing_helpers"
	"store"
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
	func(s store.Store) { _ = s }(&Redis{})
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
	val, ok := rval.(*String)
	if !ok {
		t.Fatal("expected value of type stringValue, got %T", rval)
	}
	testing_helpers.AssertEqual(t, "value", expected, val)
}

func TestSetRawKey(t *testing.T) {
	r := setupRedis()
	expected := NewBoolean(true, time.Now())
	r.SetRawKey("x", expected)

	rval, exists := r.data["x"]
	if !exists {
		t.Fatalf("no value found for key 'x'")
	}
	val, ok := rval.(*Boolean)
	if !ok {
		t.Fatal("expected value of type boolValues, got %T", rval)
	}

	testing_helpers.AssertEqual(t, "val", expected, val)
}

func TestGetKeys(t *testing.T) {
	r := setupRedis()
	val := NewBoolean(true, time.Now())
	r.SetRawKey("x", val)
	r.SetRawKey("y", val)
	r.SetRawKey("z", val)

	// make set of expected keys
	expected := map[string]bool {"x": true, "y": true, "z": true}

	seen := make(map[string] bool)
	keys := r.GetKeys()
	testing_helpers.AssertEqual(t, "num keys", len(expected), len(keys))
	for _, key := range keys {
		if expected[key] && !seen[key] {
			seen[key] = true
		} else if seen[key] {
			t.Errorf("Key '%v' seen more than once", key)
		} else {
			t.Errorf("Unexpected key returned: '%v'", key)
		}
	}
}

// when reconciling different value types, not including tombstones,
// the type with the newest timestamp should overwrite the other values
func TestReconcileDifferentValueTypes(t *testing.T) {

}

// when reconciling the same data types, reconcile should
// defer to that value's reconcile function
func TestReconcileSameValueTypes(t *testing.T) {

}

// tests reconciling mismatched values
func TestReconcileValueMismatch(t *testing.T) {
	s := setupRedis()
	ts0 := time.Now()
	ts1 := ts0.Add(time.Duration(-3000))
	expected := NewString("a", ts0)
	vmap := map[string]store.Value {
		"0": expected,
		"1": NewString("b", ts1),
		"2": expected,
	}

	rawval, adjustments, err := s.Reconcile("k", vmap)

	if err != nil {
		t.Fatalf("unexpected reconciliation error: %v", err)
	}

	actual, ok := rawval.(*String)
	if !ok {
		t.Fatalf("unexpected reconciled value type: %T", rawval)
	}

	// TODO: use value equal method instead
	testing_helpers.AssertEqual(t, "reconciled value", *expected, *actual)
	testing_helpers.AssertEqual(t, "adjustment size", 1, len(adjustments))

	instructions, ok := adjustments["1"]
	if !ok {
		t.Fatalf("instruction set for '1' not found")
	}
	testing_helpers.AssertEqual(t, "num instructions", 1, len(instructions))

	instruction := instructions[0]
	testing_helpers.AssertEqual(t, "instruction cmd", "SET", instruction.Cmd)
	testing_helpers.AssertEqual(t, "instruction key", "k", instruction.Key)
	testing_helpers.AssertEqual(t, "instruction arg size", 1, len(instruction.Args))
	testing_helpers.AssertEqual(t, "instruction arg", "a", instruction.Args[0])
	testing_helpers.AssertEqual(t, "instruction ts", ts0, instruction.Timestamp)
}

// tests that attempting to reconcile an empty map
// returns an error
func TestReconcilingEmptyMap(t *testing.T) {

}

// tests reconciling a value map with only a single element
func TestReconcileSingleValue(t *testing.T) {

}

