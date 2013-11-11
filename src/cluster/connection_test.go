package cluster

import (
	"net"
	"testing"
)

// test connecting to an address that's not listening
func TestConnectionFailure(t *testing.T) {
	addr := "127.0.0.1:9999"

	conn, err := Connect(addr, 10)

	if conn != nil {
		t.Error("expected conn to be nil")
	}

	netErr, ok := err.(net.Error)
	if !ok {
		t.Errorf("expected error of type net.Error, got %T", err)
	}

	if netErr.Timeout() {
		t.Errorf("expected non timeout error")
	}
}

// test that read timeouts kill the connection
func TestReadError(t *testing.T) {
	conn := &Connection{socket:&timeoutConn{}}
	bytes := make([]byte, 5)

	size, err := conn.Read(bytes)
	if size != 0 {
		t.Errorf("expected size of 0")
	}

	if err == nil {
		t.Fatal("error not returned")
	}

	netErr, ok := err.(net.Error)
	if !ok {
		t.Errorf("expected error of type net.Error, got %T", err)
	}

	if !netErr.Timeout() {
		t.Errorf("expected timeout error")
	}

	if !conn.Closed() {
		t.Errorf("expected connection to be closed")
	}
}

// test that write timeouts kill the connection
func TestWriteError(t *testing.T) {
	conn := &Connection{socket:&timeoutConn{}}
	bytes := make([]byte, 5)

	size, err := conn.Write(bytes)
	if size != 0 {
		t.Errorf("expected size of 0")
	}

	if err == nil {
		t.Fatal("error not returned")
	}

	netErr, ok := err.(net.Error)
	if !ok {
		t.Errorf("expected error of type net.Error, got %T", err)
	}

	if !netErr.Timeout() {
		t.Errorf("expected timeout error")
	}

	if !conn.Closed() {
		t.Errorf("expected connection to be closed")
	}
}
