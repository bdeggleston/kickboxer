package cluster

import (
	"net"
)

import (
	"launchpad.net/gocheck"
)

type ConnectionPoolTest struct {}

var _ = gocheck.Suite(&ConnectionPoolTest{})

func (t *ConnectionPoolTest) TestInitialization(c *gocheck.C) {
	addr := "127.0.0.1:9999"
	pool := NewConnectionPool(addr, 30, 10000)

	c.Check(pool.size, gocheck.Equals, uint(0))
	c.Check(pool.addr, gocheck.Equals, addr)
	c.Check(pool.timeout, gocheck.Equals, int64(10000))
	c.Check(pool.maxConn, gocheck.Equals, uint(30))
	c.Check(pool.pool, gocheck.IsNil)
	c.Check(pool.tail, gocheck.IsNil)
}

// tests that open connections are returned to
// the connection pool
func (t *ConnectionPoolTest) TestOpenConnectionReturn(c *gocheck.C) {
	pool := NewConnectionPool("127.0.0.1:9999", 30, 10000)
	conn := &Connection{socket:&dumbConn{}}

	pool.Put(conn)

	c.Check(pool.size, gocheck.Equals, uint(1))
	c.Check(pool.pool, gocheck.NotNil)
	c.Check(pool.tail, gocheck.NotNil)
}

// test that closed connections are not returned
// to the connection pool
func (t *ConnectionPoolTest) TestClosedConnectionReturn(c *gocheck.C) {
	pool := NewConnectionPool("127.0.0.1:9999", 30, 10000)
	conn := &Connection{socket:&dumbConn{}, isClosed:true}

	pool.Put(conn)

	c.Check(pool.size, gocheck.Equals, uint(0))
	c.Check(pool.pool, gocheck.IsNil)
	c.Check(pool.tail, gocheck.IsNil)
}

// test that connections are closed if the
// pool is full
func (t *ConnectionPoolTest) TestFullConnectionReturn(c *gocheck.C) {
	pool := NewConnectionPool("127.0.0.1:9999", 10, 10000)
	for i:=0; i<10; i++ {
		pool.Put(&Connection{socket:&dumbConn{}})

	}
	c.Check(pool.size, gocheck.Equals, uint(10))

	conn := &Connection{socket:&dumbConn{}}
	pool.Put(conn)
	c.Check(pool.size, gocheck.Equals, uint(10))
	c.Check(conn.isClosed, gocheck.Equals, true)
}

// test that open connections are returned
func (t *ConnectionPoolTest) TestGetConn(c *gocheck.C) {
	pool := NewConnectionPool("127.0.0.1:9999", 30, 10000)
	conn := &Connection{socket:&dumbConn{}}

	pool.Put(conn)

	c.Check(pool.size, gocheck.Equals, uint(1))

	newConn, err := pool.Get()

	c.Assert(err, gocheck.IsNil)
	c.Check(pool.size, gocheck.Equals, uint(0))
	c.Check(conn, gocheck.Equals, newConn)
}

// test that connections are opened if the
// pool is empty
func (t *ConnectionPoolTest) TestGetOpensConnection(c *gocheck.C) {
	addr := "127.0.0.1:9999"
	ln, _ := unresponsiveListener(addr)
	defer ln.Close()

	pool := NewConnectionPool("127.0.0.1:9999", 10, 10000)
	c.Assert(pool.pool, gocheck.IsNil)
	conn, err := pool.Get()
	defer conn.Close()

	c.Assert(err, gocheck.IsNil)
	c.Assert(conn, gocheck.NotNil)

	c.Assert(conn.Closed(), gocheck.Equals, false)

	c.Assert(conn.socket, gocheck.FitsTypeOf, &net.TCPConn{})
}

// tests that pool counter is incremented and
// decremented properly
func (t *ConnectionPoolTest) TestPoolSize(c *gocheck.C) {
	pool := NewConnectionPool("127.0.0.1:9999", 30, 10000)

	conn := &Connection{socket:&dumbConn{}}
	pool.Put(conn)
	c.Check(pool.size, gocheck.Equals, uint(1))

	conn = &Connection{socket:&dumbConn{}}
	pool.Put(conn)
	c.Check(pool.size, gocheck.Equals, uint(2))

	pool.Get()
	c.Check(pool.size, gocheck.Equals, uint(1))
}

// tests that the tail member is tracked properly
func (t *ConnectionPoolTest) TestTailLogic(c *gocheck.C) {
	pool := NewConnectionPool("127.0.0.1:9999", 30, 10000)

	conn := &Connection{socket:&dumbConn{}}
	pool.Put(conn)
	c.Assert(pool.tail.conn, gocheck.Equals, conn)

	conn = &Connection{socket:&dumbConn{}}
	pool.Put(conn)
	c.Assert(pool.tail.conn, gocheck.Equals, conn)

	// empty the pool
	pool.Get()
	pool.Get()

	c.Check(pool.size, gocheck.Equals, uint(0))
	c.Assert(pool.tail, gocheck.IsNil)
}
