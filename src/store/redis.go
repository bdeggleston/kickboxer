package store

import (
//	"bufio"
//	"reflect"
	"strings"
	"sync"
	"time"
)

type simpleValue struct {
	data string
	time time.Time
}

// read instructions
const (
	GET		= "GET"
)

// write instructions
const (
	SET		= "SET"
	DEL		= "DEL"
)


type Redis struct {

	data map[string] Value

	// TODO: delete
	// temporary lock, used until
	// things are broken out into
	// goroutines
	lock sync.RWMutex

}

func (s *Redis) Start() error {
	return nil
}

func (s *Redis) Stop() error {
	return nil
}

func (s *Redis) ExecuteRead(cmd string, key string, args []string) (*Value, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return nil, nil
}

func (s *Redis) ExecuteWrite(cmd string, key string, args []string, timestamp time.Time) (*Value, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return nil, nil
}

func (s *Redis) IsReadCommand(cmd string) bool {
	switch strings.ToUpper(cmd) {
	case GET:
		return true
	}
	return false
}

func (s *Redis) IsWriteCommand(cmd string) bool {
	switch strings.ToUpper(cmd) {
	case SET, DEL:
		return true
	}
	return false
}
