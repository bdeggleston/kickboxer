package redis

import (
	"time"
)

// Removes the specified keys. A key is ignored if it does not exist.
// Return value: Integer reply: The number of keys that were removed.
//
// returns a list of deleted keys to be reconciled by the coordinator
// to return the number of deleted keys
func (s *Redis) del(keys []string, ts time.Time) {
	deleted := make([]string, 0, len(keys))
	for _, key := range keys {
		if _, ok := s.data[key]; ok {
			s.data[key] = newTombstoneValue(ts)
			deleted = append(deleted, key)
		}
	}
}

