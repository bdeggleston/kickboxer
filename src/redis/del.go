package redis

import (
	"time"
)

// Removes the specified keys. A key is ignored if it does not exist.
// Return value: Integer reply: The number of keys that were removed.
//
// internally, each key is deleted one at a time, and a bool value
// is returned indicating if a key was deleted, and it's timestamp
func (s *Redis) del(key string, ts time.Time) (*boolValue, error) {
	rval := &boolValue{}
	if val, exists := s.data[key]; exists {
		s.data[key] = newTombstoneValue(ts)
		rval.value = true
		rval.time = val.GetTimestamp()
	}
	return rval, nil
}

