package testing_helpers

import (
	"bytes"
	"testing"
)

func AssertEqual(t *testing.T, name string, v1 interface {}, v2 interface{}) bool {
	if v1 != v2 {
		t.Errorf("\x1b[1m\x1b[35m%v mismatch. Expecting [%v], got [%v]\x1b[0m", name, v1, v2)
		return false
	} else {
		t.Logf("%v OK: [%v]", name, v1)
	}
	return true
}

func AssertSliceEqual(t *testing.T, name string, v1 []byte, v2 []byte) bool {
	if !bytes.Equal(v1, v2) {
		t.Errorf("\x1b[1m\x1b[35m%v mismatch. Expecting [%v], got [%v]\x1b[0m", name, v1, v2)
		return false
	} else {
		t.Logf("%v OK: [%v]", name, v1)
	}
	return true
}

func AssertStringArrayEqual(t *testing.T, name string, v1 []string, v2 []string) bool {
	areEqual := true
	areEqual = areEqual && (len(v1) == len(v2))
	if areEqual {
		for i:=0; i<len(v1); i++ {
			areEqual = areEqual && (v1[i] == v2[i])
		}
	}

	if !areEqual {
		t.Errorf("\x1b[1m\x1b[35m%v mismatch. Expecting [%v], got [%v]\x1b[0m", name, v1, v2)
		return false
	} else {
		t.Logf("%v OK: [%v]", name, v1)
	}
	return true
}
