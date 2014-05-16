package cluster

/*
Helpers for testing cluster functionality
 */

import (
	"encoding/binary"
	"bytes"
	"fmt"
	"net"
	"strconv"
	"time"
)

import (
	"kvstore"
	"message"
	"node"
	"partitioner"
	"topology"
)

// ----------------- cluster setup -----------------

func setupCluster() *Cluster {
	c, err := NewCluster(
		kvstore.NewKVStore(),
		"127.0.0.1:9999",
		"Test Cluster",
		partitioner.Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
		node.NewNodeId(),
		topology.DatacenterID("DC5000"),
		3,
		partitioner.NewMD5Partitioner(),
		nil,
	)
	if err != nil {
		panic(fmt.Sprintf("Unexpected error instantiating cluster: %v", err))
	}
	return c
}

// ----------------- ring setup -----------------

// returns a ring with 10 nodes
func setupRing() *Ring {
	r := NewRing()

	for i:=0; i<10; i++ {
		n := newMockNode(
			node.NewNodeId(),
			topology.DatacenterID("DC5000"),
			partitioner.Token([]byte{0,0,byte(i),0}),
			fmt.Sprintf("N%v", i),
		)
		r.AddNode(n)
	}

	return r
}

// makes a ring of the given size, with the tokens evenly spaced
func makeRing(size int, replicationFactor uint32) *Cluster {
	c, err := NewCluster(
		kvstore.NewKVStore(),
		"127.0.0.1:9999",
		"Test Cluster",
		partitioner.Token([]byte{0,0,0,0}),
		node.NewNodeId(),
		topology.DatacenterID("DC5000"),
		replicationFactor,
		partitioner.NewMD5Partitioner(),
		nil,
	)
	if err != nil {
		panic(fmt.Sprintf("Unexpected error instantiating cluster: %v", err))
	}

	for i:=1; i<size; i++ {
		n := newMockNode(
			node.NewNodeId(),
			topology.DatacenterID("DC5000"),
			partitioner.Token([]byte{0,0,byte(i),0}),
			fmt.Sprintf("N%v", i),
		)
		c.addNode(n)
	}

	return c
}

// makes a ring of the given size, with the tokens evenly spaced
func makeLiteralRing(size int, replicationFactor uint32) *Cluster {
	p := literalPartitioner{}
	c, err := NewCluster(
		kvstore.NewKVStore(),
		"127.0.0.1:9999",
		"Test Cluster",
		p.GetToken("0000"),
		node.NewNodeId(),
		topology.DatacenterID("DC5000"),
		replicationFactor,
		p,
		nil,
	)
	if err != nil {
		panic(fmt.Sprintf("Unexpected error instantiating cluster: %v", err))
	}

	for i:=1; i<size; i++ {
		tkey := fmt.Sprintf("%04v", i * 1000)
		token := p.GetToken(tkey)
		n := NewRemoteNodeInfo(
			node.NewNodeId(),
			topology.DatacenterID("DC5000"),
			token,
			fmt.Sprintf("N%v", i),
			fmt.Sprintf("127.0.0.%v:9999", i+2),
			c,
		)
		n.isStarted = true
		c.addNode(n)
	}

	return c
}

// ----------------- datacenter setup / mocks -----------------

func setupDC(numDCs int, numNodes int) *DatacenterContainer {
	dc := NewDatacenterContainer()

	for i:=0; i<numDCs; i++ {
		dcNum := i+1
		dcid := topology.DatacenterID(fmt.Sprintf("DC%v", dcNum))
		for i:=0; i<numNodes; i++ {
			n := newMockNode(
				node.NewNodeId(),
				dcid,
				partitioner.Token([]byte{0,0,byte(i),0}),
				fmt.Sprintf("N%v", i),
			)
			dc.AddNode(n)
		}
	}
	return dc
}

// ----------------- partitioner mocks -----------------

// returns the number passed into the key,
// keys can only be stringified ints
type literalPartitioner struct {

}

func (p literalPartitioner) GetToken(key string) partitioner.Token {
	val, err := strconv.Atoi(key)
	if err != nil {
		panic(fmt.Sprintf("The given key does not convert to an integer: %v", key))
	}
	if val < 0 {
		panic(fmt.Sprintf("The given key is a negative number: %v", key))
	}
	uval := uint64(val)
	buf := &bytes.Buffer{}

	if err := binary.Write(buf, binary.BigEndian, &uval); err != nil {
		panic(fmt.Sprintf("There was an error encoding the token: %v", err))
	}
	b := buf.Bytes()
	if len(b) != 8 {
		panic(fmt.Sprintf("Expected token length of 8, got: %v", len(b)))
	}
	return partitioner.Token(b)
}

// ----------------- connection mocks -----------------

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

// times out on reads, but not on writes
type readTimeoutConn struct {
	timeoutConn
}
func (c *readTimeoutConn) Write(b []byte) (int, error) {
	return len(b), nil
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

// successor to the bi conn. a bit easier to work with
type pgmConn struct {
	fakeConn
	incoming []message.Message
	outgoing []message.Message
	outputFactory func(*pgmConn) message.Message
}

func newPgmConn() *pgmConn {
	p := &pgmConn{}
	p.incoming = make([]message.Message, 0, 10)
	p.outgoing = make([]message.Message, 0, 10)
	return p
}

// reads outgoing messages to the receiver
func (c *pgmConn) Read(b []byte) (int, error) {
	var msg message.Message
	if c.outputFactory != nil {
		msg = c.outputFactory(c)
	} else {
		msg = c.outgoing[0]
		c.outgoing = c.outgoing[1:]
	}

	buf := &bytes.Buffer{}
	if err := message.WriteMessage(buf, msg); err != nil { panic(err) }

	num, err := buf.Read(b)
	return num, err
}

// writes incoming messages
func (c *pgmConn) Write(b []byte) (int, error) {
	buf := &bytes.Buffer{}
	num, err := buf.Write(b)
	msg, err := message.ReadMessage(buf)
	if err != nil { panic(err) }

	if c.incoming == nil {
		c.incoming = make([]message.Message, 0, 10)
	}
	c.incoming = append(c.incoming, msg)
	return num, err
}

func (c *pgmConn) getIncomingMessages() []message.Message {
	return c.incoming
}

func (c *pgmConn) addOutgoingMessage(m message.Message) {
	if c.outgoing == nil {
		c.outgoing = make([]message.Message, 0, 10)
	}
	c.outgoing = append(c.outgoing, m)
}



