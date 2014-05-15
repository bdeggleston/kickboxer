package db

import (
	"partitioner"
)

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

	// TODO: need a rb tree here
	Columns []Column

	// indicates that the key includes
	// data loaded from db
	loaded bool

	// set to true if data on the key has been
	// modified
	dirty bool
}

