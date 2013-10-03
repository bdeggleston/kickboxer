package cluster

import (
	"testing"
)

// tests that open connections are returned to
// the connection pool
func TestOpenConnectionReturn(t *testing.T) {

}

// test that closed connections are not returned
// to the connection pool
func TestClosedConnectionReturn(t *testing.T) {

}

// test that connections are closed if the
// pool is full
func TestFullConnectionReturn(t *testing.T) {

}

// test that open connections are returned
func TestGetConn(t *testing.T) {

}

// test that connections are opened if the
// pool is empty
func TestGetOpensConnection(t *testing.T) {

}
// tests that pool counter is incremented and
// decremented properly
func TestPoolSize(t *testing.T) {

}

// tests that the tail member is tracked properly
func TestTailLogic(t *testing.T) {

}
