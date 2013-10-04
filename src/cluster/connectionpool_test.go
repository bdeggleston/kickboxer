package cluster

import (
	"net"
	"testing"
)

// tests that the connection pool is initialized as expected
func TestPoolInitialization(t *testing.T) {
	addr := "127.0.0.1:9999"

	pool := NewConnectionPool(addr, 30, 10000)

	//check pool initialization
	if pool.size != 0 {
		t.Errorf("pool size 0 expected, %v found", pool.size)
	}
	if pool.addr != addr {
		t.Errorf("pool addr %v expected, %v found", addr, pool.addr)
	}
	if pool.timeout != 10000 {
		t.Errorf("pool timeout 10000 expected, %v found", pool.timeout)
	}
	if pool.maxConn != 30 {
		t.Errorf("pool maxConn 30 expected, %v found", pool.maxConn)
	}
	if pool.pool != nil {
		t.Errorf("nil pool pointer expected")
	}
	if pool.tail != nil {
		t.Errorf("nil tail pointer expected")
	}
}

// tests that open connections are returned to
// the connection pool
func TestOpenConnectionReturn(t *testing.T) {

	pool := NewConnectionPool("127.0.0.1:9999", 30, 10000)
	conn := &Connection{socket:&dumbConn{}}

	pool.Put(conn)

	if pool.size != 1 {
		t.Errorf("pool size 1 expected, %v found", pool.size)
	}
	if pool.pool == nil {
		t.Errorf("unexpected nil pool pointer found")
	}
	if pool.tail == nil {
		t.Errorf("unexpected nil tail pointer found")
	}
}

// test that closed connections are not returned
// to the connection pool
func TestClosedConnectionReturn(t *testing.T) {

	pool := NewConnectionPool("127.0.0.1:9999", 30, 10000)
	conn := &Connection{socket:&dumbConn{}, isClosed:true}

	pool.Put(conn)

	if pool.size != 0 {
		t.Errorf("pool size 0 expected, %v found", pool.size)
	}
	if pool.pool != nil {
		t.Errorf("nil pool pointer expected")
	}
	if pool.tail != nil {
		t.Errorf("nil tail pointer expected")
	}
}

// test that connections are closed if the
// pool is full
func TestFullConnectionReturn(t *testing.T) {
	pool := NewConnectionPool("127.0.0.1:9999", 10, 10000)
	for i:=0; i<10; i++ {
		pool.Put(&Connection{socket:&dumbConn{}})

	}
	if pool.size != 10 {
		t.Errorf("pool size 10 expected, %v found", pool.size)
	}

	conn := &Connection{socket:&dumbConn{}}
	pool.Put(conn)

	if pool.size != 10 {
		t.Errorf("pool size 10 expected, %v found", pool.size)
	}
	if !conn.isClosed {
		t.Errorf("connection was not closed")
	}
}

// test that open connections are returned
func TestGetConn(t *testing.T) {
	pool := NewConnectionPool("127.0.0.1:9999", 30, 10000)
	conn := &Connection{socket:&dumbConn{}}

	pool.Put(conn)

	if pool.size != 1 {
		t.Errorf("pool size 1 expected, %v found", pool.size)
	}

	newConn, err := pool.Get()

	if err != nil {
		t.Fatalf("unexpected error returned: %v", err)
	}
	if pool.size != 0 {
		t.Errorf("pool size 0 expected, %v found", pool.size)
	}
	if conn != newConn {
		t.Errorf("unexpected connection returned")
	}

}

// test that connections are opened if the
// pool is empty
func TestGetOpensConnection(t *testing.T) {
	addr := "127.0.0.1:9999"
	ln, _ := unresponsiveListener(addr)
	defer ln.Close()

	pool := NewConnectionPool("127.0.0.1:9999", 10, 10000)
	if pool.pool != nil {
		t.Fatal("empty pool expected")
	}
	conn, err := pool.Get()
	if err != nil {
		t.Fatalf("unexpected error returned: %v", err)
	}

	if conn == nil {
		t.Fatalf("nil connection returned")
	}

	if conn.Closed() {
		t.Fatalf("connection unexpectedly closed")
	}
	defer conn.Close()

	if _, ok := conn.socket.(*net.TCPConn); !ok {
		t.Errorf("conn.socket of type *new.TCPConn expected, got %T", conn.socket)
	}
}

// tests that pool counter is incremented and
// decremented properly
func TestPoolSize(t *testing.T) {
	pool := NewConnectionPool("127.0.0.1:9999", 30, 10000)

	conn := &Connection{socket:&dumbConn{}}
	pool.Put(conn)
	if pool.size != 1 {
		t.Errorf("pool size 1 expected, %v found", pool.size)
	}

	conn = &Connection{socket:&dumbConn{}}
	pool.Put(conn)
	if pool.size != 2 {
		t.Errorf("pool size 2 expected, %v found", pool.size)
	}

	pool.Get()
	if pool.size != 1 {
		t.Errorf("pool size 1 expected, %v found", pool.size)
	}


}

// tests that the tail member is tracked properly
func TestTailLogic(t *testing.T) {

	pool := NewConnectionPool("127.0.0.1:9999", 30, 10000)

	conn := &Connection{socket:&dumbConn{}}
	pool.Put(conn)
	if pool.tail.conn != conn {
		t.Errorf("unexpected connection found on tail")
	}

	conn = &Connection{socket:&dumbConn{}}
	pool.Put(conn)
	if pool.tail.conn != conn {
		t.Errorf("unexpected connection found on tail")
	}

	// empty the pool
	pool.Get()
	pool.Get()

	if pool.size != 0 {
		t.Errorf("pool size 0 expected, %v found", pool.size)
	}
	if pool.tail != nil {
		t.Errorf("nil tail expected, %v found", pool.tail)
	}
}
