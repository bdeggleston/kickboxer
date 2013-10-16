package store

import (
//	"bufio"
//	"reflect"
	"time"
)

type simpleValue struct {
	data string
	time time.Time
}


type Redis struct {

	data map[string] simpleValue

}

func (s *Redis) Start() error {
	return nil
}

func (s *Redis) Stop() error {
	return nil
}

