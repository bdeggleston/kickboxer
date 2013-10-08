/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/2/13
 * Time: 10:05 PM
 * To change this template use File | Settings | File Templates.
 */
package cluster

import (
	"bytes"
	"net"
	"testing"
	"time"
)

func unresponsiveListener(addr string) (net.Listener, error) {
	return net.Listen("tcp", addr)
}

// implements part of net.Conn, Read and Write need to be implemented
// by inheriting structs
type fakeConn struct {
	isClosed bool
}
func (c *fakeConn) Close() error {
	c.isClosed = true
	return nil
}
func (c *fakeConn) LocalAddr() net.Addr { return nil }
func (c *fakeConn) RemoteAddr() net.Addr { return nil }
func (c *fakeConn) SetDeadline(_ time.Time) error { return nil }
func (c *fakeConn) SetReadDeadline(_ time.Time) error { return nil }
func (c *fakeConn) SetWriteDeadline(_ time.Time) error { return nil }

type timeoutError struct {}
func (e *timeoutError) Error() string {return "timeoutError"}
func (e *timeoutError) Temporary() bool {return false}
func (e *timeoutError) Timeout() bool {return true}

// mock connection that returns timeouts on reads and writes
type timeoutConn struct {
	fakeConn
}
func (c *fakeConn) Read(_ []byte) (int, error) {
	return 0, &timeoutError{}
}
func (c *fakeConn) Write(_ []byte) (int, error) {
	return 0, &timeoutError{}
}


// mock connection that returns one byte on reads
type dumbConn struct {
	fakeConn
}

func (c *dumbConn) Read(b []byte) (int, error) {
	b[0] = byte(0)
	return 1, nil
}

func (c *dumbConn) Write(b []byte) (int, error) {
	return len(b), nil
}


// mock connection the reads out whatever was
// last written into it
type echoConn struct {
	fakeConn
	data []byte
}

func (c *echoConn) Read(b []byte) (int, error) {
	//TODO: support smaller buffers
	copy(c.data, b)
	return len(c.data), nil
}

func (c *echoConn) Write(b []byte) (int, error) {
	c.data = make([]byte, len(b))
	copy(b, c.data)
	return len(b), nil
}

// mock connection with 2 buffers, one for input,
// one for output
type biConn struct {
	fakeConn
	input []*bytes.Buffer
	output []*bytes.Buffer
	inIdx int
	outIdx int
}

func newBiConn(numIn, numOut int) *biConn {
	c := &biConn{
		input:make([]*bytes.Buffer, numIn),
		output:make([]*bytes.Buffer, numOut),
	}
	c.inIdx = 0
	c.outIdx = 0
	for i:=0;i<numIn;i++ {
		c.input[i] = &bytes.Buffer{}
	}
	for i:=0;i<numOut;i++ {
		c.output[i] = &bytes.Buffer{}
	}
	return c
}

func (c *biConn) Read(b []byte) (int, error) {
	num, err := c.input[c.inIdx].Read(b)
	c.inIdx++
	return num, err
}

func (c *biConn) Write(b []byte) (int, error) {
	num, err := c.output[c.outIdx].Write(b)
	c.outIdx++
	return num, err
}

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
