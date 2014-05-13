package cluster

import (
	"io"
	"strings"
)

import (
	"launchpad.net/gocheck"
)

import (
	"kvstore"
	"message"
	"node"
)

type ServerTest struct {
	ring *Ring
}

var _ = gocheck.Suite(&ServerTest{})

func (t *ServerTest) TestGetUnknownNode(c *gocheck.C) {
	conn := newBiConn(2,1)

	// write input messages
	connectMessage := &ConnectionRequest{PeerData{
		NodeId:node.NewNodeId(),
		Addr:"127.0.0.1:9999",
		Name:"Test Node",
		Token:Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
	}}
	err := message.WriteMessage(conn.input[0], connectMessage)
	c.Assert(err, gocheck.IsNil)

	//	closeMessage := &closeConnection{}
	//	if err := message.WriteMessage(conn.input[1], closeMessage); err != nil {
	//		t.Fatalf("Unexpected error writing close message: %v", err)
	//	}

	// create cluster and peer server
	token := Token([]byte{4,5,6,7,0,1,2,3,4,5,6,7,0,1,2,3})
	cluster, err := NewCluster(
		kvstore.NewKVStore(),
		"127.0.0.1:9999",
		"TestCluster",
		token,
		node.NewNodeId(),
		"DC1",
		3,
		NewMD5Partitioner(),
		nil,
	)
	c.Assert(err, gocheck.IsNil)

	server := &PeerServer{cluster:cluster}
	err = server.handleConnection(conn)
	c.Assert(err, gocheck.Equals, io.EOF)

	// read output messages
	c.Assert(len(conn.output), gocheck.Equals, 1)
	rawAcceptMessage, err := message.ReadMessage(conn.output[0])
	c.Assert(err, gocheck.IsNil)

	// verify output
	c.Assert(rawAcceptMessage, gocheck.FitsTypeOf, &ConnectionAcceptedResponse{})
	acceptMessage := rawAcceptMessage.(*ConnectionAcceptedResponse)

	c.Check(acceptMessage.Name, gocheck.Equals, cluster.GetName())
	c.Check(acceptMessage.NodeId, gocheck.Equals, cluster.GetNodeId())
	c.Check(acceptMessage.Token, gocheck.DeepEquals, cluster.GetToken())
}

// tests sending a message other than connection request
// as a first message results in a closed connection
func (t *ServerTest) TestServerConnectionFailure(c *gocheck.C) {
	conn := newBiConn(1,1)

	// write input messages
	connectMessage := &ReadRequest{Cmd:"GET", Key:"A", Args:[]string{"B"}}
	err := message.WriteMessage(conn.input[0], connectMessage)
	c.Assert(err, gocheck.IsNil)

	// create cluster and peer server
	token := Token([]byte{4,5,6,7,0,1,2,3,4,5,6,7,0,1,2,3})
	cluster, err := NewCluster(
		kvstore.NewKVStore(),
		"127.0.0.1:9999",
		"TestCluster",
		token,
		node.NewNodeId(),
		"DC1",
		3,
		NewMD5Partitioner(),
		nil,
	)
	c.Assert(err, gocheck.IsNil)

	server := &PeerServer{cluster:cluster}
	err = server.handleConnection(conn)
	c.Assert(err, gocheck.NotNil)

	// read output messages
	rawAcceptMessage, err := message.ReadMessage(conn.output[0])
	c.Assert(err, gocheck.IsNil)

	// verify output
	c.Assert(rawAcceptMessage, gocheck.FitsTypeOf, &ConnectionRefusedResponse{})
	refusalMessage := rawAcceptMessage.(*ConnectionRefusedResponse)

	c.Check(strings.Contains(refusalMessage.Reason, "ConnectionRequest expected"), gocheck.Equals, true)
}

func (t *ServerTest) TestServerNodeRegistrationOnConnection(c *gocheck.C) {
	conn := newBiConn(2,1)

	// write input messages
	connectMessage := &ConnectionRequest{PeerData{
		NodeId:node.NewNodeId(),
		DCId:"DC1",
		Addr:"127.0.0.1:9999",
		Name:"Test Node",
		Token:Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
	}}

	err := message.WriteMessage(conn.input[0], connectMessage)
	c.Assert(err, gocheck.IsNil)

	// create cluster and peer server
	token := Token([]byte{4,5,6,7,0,1,2,3,4,5,6,7,0,1,2,3})
	cluster, err := NewCluster(
		kvstore.NewKVStore(),
		"127.0.0.1:9999",
		"TestCluster",
		token,
		node.NewNodeId(),
		"DC1",
		3,
		NewMD5Partitioner(),
		nil,
	)
	c.Assert(err, gocheck.IsNil)

	// sanity check
	_, err = cluster.ring.GetNode(connectMessage.NodeId)
	c.Assert(err, gocheck.NotNil)

	server := &PeerServer{cluster:cluster}
	err = server.handleConnection(conn)
	c.Assert(err, gocheck.Equals, io.EOF)

	_, err = cluster.ring.GetNode(connectMessage.NodeId)
	c.Assert(err, gocheck.IsNil)
}

