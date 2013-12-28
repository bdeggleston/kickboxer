package consensus

import (
	"testing"
)

import (
	"testing_helpers"
)

func TestInstanceMergeAttributes(t *testing.T) {

}

func TestInstanceIDSetEqual(t *testing.T) {
	ids := []InstanceID{NewInstanceID(), NewInstanceID()}
	set0 := NewInstanceIDSet(ids)
	set1 := NewInstanceIDSet(ids)
	if !set0.Equal(set1) {
		t.Errorf("Expected true, got false")
	}

}

func TestInstanceIDSetUnion(t *testing.T) {
	ids := []InstanceID{NewInstanceID(), NewInstanceID(), NewInstanceID()}
	set0 := NewInstanceIDSet(ids[:2])
	set1 := NewInstanceIDSet(ids[1:])

	testing_helpers.AssertEqual(t, "set0 size", 2, len(set0))
	testing_helpers.AssertEqual(t, "set1 size", 2, len(set1))

	set2 := set0.Union(set1)
	testing_helpers.AssertEqual(t, "set2 size", 3, len(set2))
}
