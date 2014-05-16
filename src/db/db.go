package db

import (
	"io"
)

/**
	Learn more about:
		Page Cache
		Page Faults
		Disk Cache
		Memory Mapped files (how to read stuff without a bunch of seeks happening?)

	http://static.googleusercontent.com/media/research.google.com/en/us/archive/bigtable-osdi06.pdf


	token:key:val_type => {col_name: {val, timestamp/version+instance_id}}
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

type Slice struct {
	Start string
	Stop string
	Columns []io.Reader
}

