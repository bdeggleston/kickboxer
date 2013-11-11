package kvstore

import (
	"store"
	"time"
	"fmt"
)

func (s *KVStore) validateSet(key string, args []string, timestamp time.Time) error {
	_ = key
	if len(args) != 1 {
		return fmt.Errorf("incorrect number of args for SET. Expected 1, got %v", len(args))
	}
	if timestamp.IsZero() {
		return fmt.Errorf("DEL Got zero timestamp")
	}
	return nil
}

// Set key to hold the string value. If key already holds a value, it is overwritten,
// regardless of its type. Any previous time to live associated with the key is discarded
// on successful SET operation.
func (s *KVStore) set(key string, val string, ts time.Time) (store.Value) {
	existing, exists := s.data[key]
	if exists && ts.Before(existing.GetTimestamp()) {
		return existing
	}
	value := NewString(val, ts)
	s.data[key] = value
	return value
}

