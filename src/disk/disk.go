package disk

import (
	"time"
)

import (
	"partitioner"
)

/**
	Learn more about:
		Page Cache
		Page Faults
		Disk Cache
		Memory Mapped files (how to read stuff without a bunch of seeks happening?)

	http://static.googleusercontent.com/media/research.google.com/en/us/archive/bigtable-osdi06.pdf


	token:key:val_type => {col_name: {val, timestamp/version}}
 */

/**
How to:
	reconcile values where operations may be committed out of order of their timestamps. With paxos
	transaction, it's possible that the commit/execution order doesn't conform to the timestamp.

	possible solutions:
		workout an update counter for a given key/col. However, this would probably require a
		read before every write
		use memtable versions. Each new memtable increments a counter for it's table, and when it's
		flushed all of it's keys/columns has that set over a timestamp. This would be useful for
		internal use, but would make reconciliation / streaming between nodes more difficult, since
		memtable versions cannot be assumed to be in sync across different nodes. However, since the
		counter tables will only be used for tables managed by paxos, the most recently executed
		instance id could be used instead.

 */

type Table interface {
	GetKey(token partitioner.Token, key string) *Key
	Start() error
	Stop() error
}

type IKey interface {
	GetSlice(sliceStart string, sliceStop string) []Column
	Mutate(column string, value string)
	Delete()
	DeleteSlice(sliceStart string, sliceStop string)
}

type Key struct {
	Token partitioner.Token
	Key string
	Type byte
	Columns []Column
}

type Column struct {
	Col string
	Val string
	Version uint64
	Timestamp time.Time
}

type CommitLog struct {

}

type MemTable struct {
	Keys map[string]*Key
}

type SSTableWriter struct {

}

type SSTableReader struct {

}

type SSTableMerger struct {

}
