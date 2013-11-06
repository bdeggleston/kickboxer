package redis

import (
	"store"
)

// Get the value of key. If the key does not exist the special value nil is returned.
// An error is returned if the value stored at key is not a string, because GET only handles string values.
func (s *Redis) get(key string) (store.Value, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	// TODO: check that the returned value is a string or tombstone value
	return s.data[key], nil
}

