package db

import (
	"io"
)


import (
	"partitioner"
)

type Table interface {
	// returns a key (maybe initialized, maybe not)
	GetKey(token partitioner.Token, key string) *Key
	Start() error
	Stop() error
}

type Table2 interface {
	// returns all columns for the given key
	GetKey(token partitioner.Token, key string) [][]io.Reader
	Start() error
	Stop() error
}
