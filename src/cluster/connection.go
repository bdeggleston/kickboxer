/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/2/13
 * Time: 9:17 PM
 * To change this template use File | Settings | File Templates.
 */
package cluster

import (
	"net"
	"sync"
	"time"
)


// encapsulates a remote connection
type Connection struct {
	socket net.Conn
	timeout int64
	isClosed bool
}

// connects and returns a new connection
// timeout is the number of milliseconds until the connection
// attempt is aborted
func Connect(addr string, timeout int64) (*Connection, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Duration(timeout) * time.Millisecond)
	if err != nil {
		return nil, err
	}
	return &Connection{socket:conn, timeout:timeout}, nil
}

// this implements the io.Reader interface
func (c *Connection) Read(b []byte) (int, error) {
	size, err := c.socket.Read(b)
	if err != nil {
		c.isClosed = true
	}
	return size, err
}

// this implements the io.Writer interface
func (c *Connection) Write(b []byte) (int, error) {
	size, err := c.socket.Write(b)
	if err != nil {
		c.isClosed = true
	}
	return size, err
}

// closes the connection
func (c *Connection) Close() {
	c.socket.Close()
	c.isClosed = true
}

func (c *Connection) Closed() bool {
	return c.isClosed
}

// linked list for connection pool
type connHolder struct {
	conn *Connection
	next *connHolder
}

// pools connections to a single host
type ConnectionPool struct {
	sync.RWMutex

	addr string
	size uint
	timeout int64
	maxConn uint

	pool *connHolder
	tail *connHolder
}


// creates a connection pool for the given address and max size
// if the max size is 0, 10 is used, if the given timeout is 0
// or less, 10 seconds is used
func NewConnectionPool(addr string, maxConn uint, timeout int64) *ConnectionPool {
	if maxConn == 0 {
		maxConn = 10
	}

	// default connection timeout is 10 seconds
	if timeout <= 0 {
		timeout = 10000
	}
	return &ConnectionPool{addr:addr, maxConn:maxConn, timeout:timeout}
}

// returns a connection to the pool
func (cp *ConnectionPool) Put(conn *Connection) {
	cp.Lock()
	defer cp.Unlock()

	if cp.size < cp.maxConn {
		holder := &connHolder{conn:conn}
		if cp.pool == nil {
			cp.pool = holder
			cp.tail = holder
		} else {
			cp.tail.next = holder
			cp.tail = holder
		}
		cp.size++
	} else {
		conn.Close()
	}
}

// gets a connection from the pool, creating one if the
// pool is empty
func (cp *ConnectionPool) Get() (conn *Connection, err error) {
	cp.Lock()
	defer cp.Unlock()

	if cp.pool == nil {
		return Connect(cp.addr, 1)
	} else {
		// remove a connection from the pool
		conn = cp.pool.conn
		cp.pool = cp.pool.next
		cp.size--

		// kill the tail if the pool is empty
		if cp.pool == nil {
			cp.tail = nil
		}
	}
	return
}


