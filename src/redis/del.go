package redis

import (
	"fmt"
	"time"

	"redis/values"
)

func (s *Redis) validateDel(key string, args []string, timestamp time.Time) error {
	_ = key
	if len(args) != 0 {
		return fmt.Errorf("DEL takes 0 args, %v found", len(args))
	}
	if timestamp.IsZero() {
		return fmt.Errorf("DEL Got zero timestamp")
	}
	return nil
}

// Removes the specified keys. A key is ignored if it does not exist.
// Return value: Integer reply: The number of keys that were removed.
//
// internally, each key is deleted one at a time, and a bool value
// is returned indicating if a key was deleted, and the previos value's
// timestamp if one was found
func (s *Redis) del(key string, ts time.Time) (*Boolean, error) {
	var rval *Boolean
	if val, exists := s.data[key]; exists {
		s.data[key] = NewTombstone(ts)
		rval = NewBoolean(true, val.GetTimestamp())
	} else {
		rval = NewBoolean(false, time.Time{})
	}
	return rval, nil
}

