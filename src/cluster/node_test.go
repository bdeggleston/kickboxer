package cluster

import (
	"launchpad.net/gocheck"
)

import (
	"message"
	"node"
)


/************** local tests **************/

type LocalNodeTest struct {}

var _ = gocheck.Suite(&LocalNodeTest{})

// tests that starting the local node starts the local store
func (t *LocalNodeTest) TestStartingConnectsToStore(c *gocheck.C) {

}

// tests that calling ExecuteRead calls the proper store method
func (t *LocalNodeTest) TestReadRequestCallsCorrectStoreMethod(c *gocheck.C) {

}

// tests that calling ExecuteWrite calls the proper store method
func (t *LocalNodeTest) TestWriteRequestCallsCorrectStoreMethod(c *gocheck.C) {

}

/************** remote tests **************/

type RemoteNodeTest struct {}

var _ = gocheck.Suite(&RemoteNodeTest{})

// tests that calling Start on a remote node initiates
// a connection to the remote peer server, copies
// it's data, and sets the node's status to UP
func (t *RemoteNodeTest) TestStartingConnectsToPeer(c *gocheck.C) {
	// write a connection acceptance message
	sock := newBiConn(1, 1)
	response := &ConnectionAcceptedResponse{
		NodeId:node.NewNodeId(),
		Name:"Ghost",
		Token:Token([]byte{0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,3}),
	}
	message.WriteMessage(sock.input[0], response)

	cluster := setupCluster()
	n := NewRemoteNode("127.0.0.2:9998", cluster)
	conn := &Connection{socket:sock}
	n.pool.Put(conn)

	// pre startup sanity check
	c.Check(conn.completedHandshake, gocheck.Equals, false)
	c.Check(n.name, gocheck.Equals, "")
	c.Check(n.id, gocheck.Equals, node.NodeId{})
	c.Check(n.status, gocheck.Equals, NODE_INITIALIZING)
	c.Check(len(n.token), gocheck.DeepEquals, 0)

	// start the node
	err := n.Start()
	c.Assert(err, gocheck.IsNil)

	// check that values were saved properly
	c.Check(conn.completedHandshake, gocheck.Equals, true)
	c.Check(n.name, gocheck.Equals, response.Name)
	c.Check(n.id, gocheck.Equals, response.NodeId)
	c.Check(n.status, gocheck.Equals, NODE_UP)
	c.Check(n.token, gocheck.DeepEquals, response.Token)
}

// tests that sending and receiving messages works as
// expected
func (t *RemoteNodeTest) TestMessageSendingSuccessCase(c *gocheck.C) {
	sock := newBiConn(1, 1)
	expected := &DiscoverPeerResponse{Peers:[]*PeerData{}}

	message.WriteMessage(sock.input[0], expected)

	cluster := setupCluster()
	n := NewRemoteNode("127.0.0.2:9998", cluster)
	n.status = NODE_UP
	conn := &Connection{socket:sock}
	conn.SetHandshakeCompleted()
	n.pool.Put(conn)

	request := &DiscoverPeersRequest{NodeId:n.GetId()}
	rawResponse, err := n.SendMessage(request)
	c.Assert(err, gocheck.IsNil)

	response := rawResponse.(*DiscoverPeerResponse)
	c.Check(len(response.Peers), gocheck.Equals, 0)
	c.Check(n.status, gocheck.Equals, NODE_UP)
}

// tests that a node is marked as down if sending
// a message fails
func (t *RemoteNodeTest) TestMessageSendingFailureCase(c *gocheck.C) {
	cluster := setupCluster()
	n := NewRemoteNode("127.0.0.2:9998", cluster)
	n.status = NODE_UP
	conn := &Connection{socket:&readTimeoutConn{}}
	conn.SetHandshakeCompleted()
	n.pool.Put(conn)

	request := &DiscoverPeersRequest{NodeId:n.GetId()}
	response, err := n.SendMessage(request)
	c.Assert(err, gocheck.NotNil)
	c.Assert(response, gocheck.IsNil)
	c.Assert(n.status, gocheck.Equals, NODE_DOWN)
}

// tests calling execute query sends correct message
func (t *RemoteNodeTest) TestExecuteQueryMessage(c *gocheck.C) {
	// TODO: this
	c.Skip("TODO: this")

}
