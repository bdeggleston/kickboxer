package testing_helpers

import (
	"bytes"
	"testing"
)

func AssertEqual(t *testing.T, name string, v1 interface {}, v2 interface{}) {
	if v1 != v2 {
		t.Errorf("\x1b[1m\x1b[35m%v mismatch. Expecting [%v], got [%v]\x1b[0m", name, v1, v2)
	} else {
		t.Logf("%v OK: [%v]", name, v1)
	}
}

func AssertSliceEqual(t *testing.T, name string, v1 []byte, v2 []byte) {
	if !bytes.Equal(v1, v2) {
		t.Errorf("\x1b[1m\x1b[35m%v mismatch. Expecting [%v], got [%v]\x1b[0m", name, v1, v2)
	} else {
		t.Logf("%v OK: [%v]", name, v1)
	}
}