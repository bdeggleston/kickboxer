package kvstore

import (
	"testing"
	"store"
)

// returns a redis instance with defaults set
func setupKVStore() *KVStore {
	return NewKVStore()
}

func assertEqualValue(t *testing.T, name string, v1 store.Value, v2 store.Value) {
	if !v1.Equal(v2) {
		t.Errorf("\x1b[1m\x1b[35m%v mismatch. Expecting [%v], got [%v]\x1b[0m", name, v1, v2)
	} else {
		t.Logf("%v OK: [%v]", name, v1)
	}
}
