package db

import (
	"time"
)

import (
	"consensus"
	"store"
)

type Column struct {
	// column data
	Col string
	Val store.Value

	// reconciliation data
	Version uint64
	Timestamp time.Time

	// the paxos instance that made the last write
	LastInstance consensus.InstanceID
}

