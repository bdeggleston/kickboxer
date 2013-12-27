package consensus

import (
	"time"
)

type Command struct {
	// the actual instruction to be executed
	Cmd       string
	Key       string
	Args      []string
	Timestamp time.Time

	instance *Instance
}
