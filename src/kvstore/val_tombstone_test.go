package kvstore

import (
	"testing"
	"time"

	"testing_helpers"
	"store"
)

func TestTombstoneValue(t *testing.T) {
	s := setupKVStore()
	src := NewTombstone(time.Now())

	b, err := s.SerializeValue(src)
	if err != nil {
		t.Fatalf("Unexpected serialization error: %v", err)
	}

	val, vtype, err := s.DeserializeValue(b)
	if err != nil {
		t.Fatalf("Unexpected deserialization error: %v", err)
	}
	if vtype != TOMBSTONE_VALUE {
		t.Fatalf("Unexpected value type enum: %v", vtype)
	}
	dst, ok := val.(*Tombstone)
	if !ok {
		t.Fatalf("Unexpected value type: %T", val)
	}

	testing_helpers.AssertEqual(t, "time", src.time, dst.time)
}

// tests that the tombstone struct satisfies the
// value interface
func TestTombstoneInterface(t *testing.T) {
	func (store.Value){}(NewTombstone(time.Now()))
}

// tests that mismatched values are reconciled and
// corrected as expected
func TestTombstoneMismatchReconciliation(t *testing.T) {
	ts0 := time.Now()
	ts1 := ts0.Add(time.Duration(-3000))
	expected := NewTombstone(ts0)
	values := []store.Value{expected, NewTombstone(ts1), expected}

	ractual, adjustments, err := setupKVStore().Reconcile("k", values)

	if err != nil {
		t.Fatalf("unexpected reconciliation error: %v", err)
	}

	actual, ok := ractual.(*Tombstone)
	if !ok { t.Fatalf("Unexpected return value type: %T", ractual) }

	assertEqualValue(t, "reconciled value", expected, actual)
	testing_helpers.AssertEqual(t, "adjustment size", len(values), len(adjustments))
	testing_helpers.AssertEqual(t, "num instructions", 0, len(adjustments[0]))
	testing_helpers.AssertEqual(t, "num instructions", 0, len(adjustments[2]))

	instructions := adjustments[1]
	testing_helpers.AssertEqual(t, "num instructions", 1, len(instructions))

	instruction := instructions[0]
	expected_instr := store.Instruction{Cmd:"DEL", Key:"k", Args:[]string{}, Timestamp:ts0}
	if !expected_instr.Equal(instruction) {
		t.Fatalf("unexpected instruction value. Expected: [%v], got: [%v]", expected_instr, instruction)
	}
}

// should set values of different types to the value
// with the largest timestamp
func TestTombstoneMultiTypeReconciliation(t *testing.T) {
	ts0 := time.Now()
	ts1 := ts0.Add(time.Duration(-3000))
	expected := NewTombstone(ts0)
	values := []store.Value{expected, NewString("a", ts1)}

	ractual, adjustments, err := setupKVStore().Reconcile("k", values)

	if err != nil {
		t.Fatalf("unexpected reconciliation error: %v", err)
	}

	actual, ok := ractual.(*Tombstone)
	if !ok { t.Fatalf("Unexpected return value type: %T", ractual) }

	assertEqualValue(t, "reconciled value", expected, actual)
	testing_helpers.AssertEqual(t, "adjustment size", len(values), len(adjustments))
	testing_helpers.AssertEqual(t, "num instructions", 0, len(adjustments[0]))

	instructions := adjustments[1]
	testing_helpers.AssertEqual(t, "num instructions", 1, len(instructions))

	instruction := instructions[0]
	expected_instr := store.Instruction{Cmd:"DEL", Key:"k", Args:[]string{}, Timestamp:ts0}
	if !expected_instr.Equal(instruction) {
		t.Fatalf("unexpected instruction value. Expected: [%v], got: [%v]", expected_instr, instruction)
	}
}

// should return the correct value and no adjustment
// instructions if all of the values match
func TestTombstoneNoOpReconciliation(t *testing.T) {
	ts0 := time.Now()
	expected := NewTombstone(ts0)
	values := []store.Value {expected, expected, expected}

	ractual, adjustments, err := setupKVStore().Reconcile("k", values)

	if err != nil {
		t.Fatalf("unexpected reconciliation error: %v", err)
	}

	actual, ok := ractual.(*Tombstone)
	if !ok { t.Fatalf("Unexpected return value type: %T", ractual) }

	assertEqualValue(t, "reconciled value", expected, actual)
	testing_helpers.AssertEqual(t, "adjustment size", len(values), len(adjustments))
	for _, adjustment := range adjustments {
		testing_helpers.AssertEqual(t, "adjustment size", 0, len(adjustment))
	}
}

// tests the tombstone value's equality method
func TestTombstoneEquality(t *testing.T) {
	t0 := time.Now()
	v0 := NewTombstone(t0)

	testing_helpers.AssertEqual(t, "equal value", true, v0.Equal(NewTombstone(t0)))
	testing_helpers.AssertEqual(t, "unequal timestamp", false, v0.Equal(NewTombstone(t0.Add(4))))
	testing_helpers.AssertEqual(t, "unequal type", false, v0.Equal(NewString("asdf", t0)))
}
