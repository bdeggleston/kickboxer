package redis

import (
	"store"
	"time"
)

// Set key to hold the string value. If key already holds a value, it is overwritten,
// regardless of its type. Any previous time to live associated with the key is discarded
// on successful SET operation.
func (s *Redis) set(key string, val string, ts time.Time) (store.Value) {
	existing, exists := s.data[key]
	if exists && ts.Before(existing.GetTimestamp()) {
		return existing
	}
	value := newStringValue(val, ts)
	s.data[key] = value
	return value
}

