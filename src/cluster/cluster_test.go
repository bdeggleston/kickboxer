package cluster

import (
	"flag"
	"fmt"
	"testing"
)

import (
	"launchpad.net/gocheck"
	logging "github.com/op/go-logging"
)

import (
	"kvstore"
	"message"
	"node"
)

var _test_loglevel = flag.String("test.loglevel", "", "the loglevel to run tests with")

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {

	// setup test suite logging
	logLevel := logging.CRITICAL
	if *_test_loglevel != "" {
		if level, err := logging.LogLevel(*_test_loglevel); err == nil {
			logLevel = level
		}
	}
	logging.SetLevel(logLevel, "cluster")

	gocheck.TestingT(t)
}

type ClusterTest struct {}

var _ = gocheck.Suite(&ClusterTest{})

// tests the cluster constructor works as expected
// and all of it's basic methods return the proper
// values
func (t *ClusterTest) TestSetup(c *gocheck.C) {
	cluster := setupCluster()
	c.Check(cluster.GetName(), gocheck.Equals, cluster.name)
	c.Check(cluster.GetNodeId(), gocheck.Equals, cluster.nodeId)
	c.Check(cluster.GetPeerAddr(), gocheck.Equals, cluster.peerAddr)
	c.Check(cluster.GetToken(), gocheck.DeepEquals, cluster.token)
}

// tests that instantiating a cluster with an invalid replication
// factor returns an error
func (t *ClusterTest) TestInvalidReplicationFactor(c *gocheck.C) {
	clstr, err := NewCluster(
		kvstore.NewKVStore(),
		"127.0.0.1:9999",
		"Test Cluster",
		Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
		node.NewNodeId(),
		DatacenterId("DC1234"),
		0,
		NewMD5Partitioner(),
		nil,
	)
	c.Assert(clstr, gocheck.IsNil)
	c.Assert(err, gocheck.NotNil)
}

func (t *ClusterTest) TestInvalidPartitioner(c *gocheck.C) {
	clstr, err := NewCluster(
		kvstore.NewKVStore(),
		"127.0.0.1:9999",
		"Test Cluster",
		Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
		node.NewNodeId(),
		DatacenterId("DC1234"),
		3,
		nil,
		nil,
	)
	c.Assert(clstr, gocheck.IsNil)
	c.Assert(err, gocheck.NotNil)
}

/************** addNode tests **************/

type AddNodeTest struct {}

var _ = gocheck.Suite(&AddNodeTest{})

// TODO: this
// tests that a node is added to the cluster if
// the cluster has not seen it yet, and starts it
// if the cluster has been started
func (t *AddNodeTest) TestAddingNewNodeToStartedCluster(c *gocheck.C) {
	c.Skip("Cluster starting not implemented yet")
}

func (t *AddNodeTest) TestAddingNode(c *gocheck.C) {
	clstr := makeRing(5, 3)
	rnode := NewRemoteNodeInfo(
		node.NewNodeId(),
		DatacenterId("DC5000"),
		Token([]byte{0,0,1,0}),
		"N1",
		"127.0.0.1:9999",
		clstr,
	)
	err := clstr.addNode(rnode)
	c.Assert(err, gocheck.IsNil)

	n, err := clstr.ring.GetNode(rnode.GetId())
	c.Assert(err, gocheck.IsNil)

	c.Check(rnode.GetId(), gocheck.Equals, n.GetId())
}

func (t *AddNodeTest) TestAddOtherDCNode(c *gocheck.C) {
	clstr := makeRing(5, 3)
	rnode := NewRemoteNodeInfo(
		node.NewNodeId(),
		DatacenterId("DC4000"),
		Token([]byte{0,0,1,0}),
		"N1",
		"127.0.0.1:9999",
		clstr,
	)

	err := clstr.addNode(rnode)
	c.Assert(err, gocheck.IsNil)

	ring, err := clstr.dcContainer.GetRing("DC4000")
	c.Assert(err, gocheck.IsNil)

	n, err := ring.getNode(rnode.GetId())
	c.Assert(err, gocheck.IsNil)

	c.Check(rnode.GetId(), gocheck.Equals, n.GetId())
}

/************** getPeerData tests **************/

type PeerDataTest struct {}

var _ = gocheck.Suite(&PeerDataTest{})

func (t *PeerDataTest) TestExpectedDataIsReturned(c *gocheck.C) {
	clstr := makeRing(5, 3)

	var data []*PeerData

	data = clstr.getPeerData()
	for _, pd := range data {
		n, err := clstr.ring.GetNode(pd.NodeId)
		c.Assert(err, gocheck.IsNil)
		c.Assert(n, gocheck.NotNil)


		c.Check(pd.Name, gocheck.Equals, n.Name())
		c.Check(pd.Addr, gocheck.Equals, n.GetAddr())
		c.Check(pd.Token, gocheck.DeepEquals, n.GetToken())
	}
}

func (t *PeerDataTest) TestExpectedDataIsReturnedFromOtherDCs(c *gocheck.C) {
	clstr := makeRing(3, 3)
	clstr.dcContainer = setupDC(3, 3)

	data := clstr.getPeerData()
	expectedNumNodes := 3+3+3+3-1
	c.Assert(len(data), gocheck.Equals, expectedNumNodes)

	nodeMap := make(map[node.NodeId]ClusterNode)
	for _, n := range clstr.ring.AllNodes() {
		if n.GetId() == clstr.GetNodeId() { continue }
		nodeMap[n.GetId()] = n
	}
	for _, n := range clstr.dcContainer.AllNodes() {
		nodeMap[n.GetId()] = n
	}

	for _, pd := range data {
		n, ok := nodeMap[pd.NodeId]
		delete(nodeMap, pd.NodeId)
		c.Assert(ok, gocheck.Equals, true)

		c.Check(pd.NodeId, gocheck.Equals, n.GetId())
		c.Check(pd.DCId, gocheck.Equals, n.GetDatacenterId())
		c.Check(pd.Name, gocheck.Equals, n.Name())
		c.Check(pd.Addr, gocheck.Equals, n.GetAddr())
		c.Check(pd.Token, gocheck.DeepEquals, n.GetToken())
	}

	c.Assert(len(nodeMap), gocheck.Equals, 0, gocheck.Commentf("Remaining nodes"))
}

func (t *PeerDataTest) TestSelfIsNotReturned(c *gocheck.C) {
	clstr := makeRing(5, 3)

	var data []*PeerData

	data = clstr.getPeerData()
	c.Assert(len(data), gocheck.Equals, 4)
	for _, pd := range data {
		c.Check(pd.NodeId, gocheck.Not(gocheck.Equals), clstr.GetNodeId())
	}
}

// TODO: this
// 2 nodes added to the cluster at the same time
// should find out about each other
func (t *PeerDataTest) TestConcurrentNodeAdditions(c *gocheck.C) {
	c.Skip("TODO: this")
}

/************** startup tests **************/

type ClusterStartupTest struct {}

var _ = gocheck.Suite(&ClusterStartupTest{})

// tests all nodes are started on cluster startup
func (t *ClusterStartupTest) TestNodesAreStarted(c *gocheck.C) {
	clstr := makeRing(10, 3)

	err := clstr.Start()
	defer clstr.Stop()
	c.Assert(err, gocheck.IsNil)

	for _, n := range clstr.ring.AllNodes() {
		c.Check(n.IsStarted(), gocheck.Equals, true)
	}
}

func (t *ClusterStartupTest) TestClusterStatusChanges(c *gocheck.C) {
	clstr := makeRing(10, 3)
	c.Assert(clstr.status, gocheck.Equals, CLUSTER_INITIALIZING)

	err := clstr.Start()
	defer clstr.Stop()
	c.Assert(err, gocheck.IsNil)

	c.Assert(clstr.status, gocheck.Equals, CLUSTER_NORMAL)
}

func (t *ClusterStartupTest) TestPeerServerIsStarted(c *gocheck.C) {
	clstr := makeRing(10, 3)
	c.Assert(clstr.peerServer.isRunning, gocheck.Equals, false)

	err := clstr.Start()
	defer clstr.Stop()
	c.Assert(err, gocheck.IsNil)

	c.Assert(clstr.peerServer.isRunning, gocheck.Equals, true)
}

/*
TODO:
	how to maintain token rings and maps without locks?
		LOCKS
			* we need to be able to run multiple requests of any type concurrently
			* we only need to be concerned with concurrent mutation of the ring state
			* with each of the peer servers connections running in their own goroutine,
			sending all messages over a single channel doesn't seem practical
			* cluster's core functionality is interacting with the node map and
			token ring. Putting all of that in a single goroutine would be clunky
			* cluster mutations should be relatively rare... so running an
			actor seems overkill

		CLUSTER CHANNELS
			* having locks everywhere is error prone. It's fairly early in the
			implementation, and has already been a dead lock condition

		SERVER CHANNELS
			* how many functions will the peer server really be calling? Having
			a channel for each one probably wouldn't be that bad
			* having a single peer server goroutine which interfaces with the cluster
			would be ok from a complexity standpoint, but this would mean that the
			request handler would essentially be single threaded

	mocking out the remote node constructors
		* wrapping a private constructor is probably the most straightforward and
		flexible approach
 */

type PeerDiscoveryTest struct {
	oldNewRemoteNode func(string, *Cluster) (*RemoteNode)

}

var _ = gocheck.Suite(&PeerDiscoveryTest{})

func (t *PeerDiscoveryTest) SetUpSuite(c *gocheck.C) {
	t.oldNewRemoteNode = newRemoteNode
}

func (t *PeerDiscoveryTest) TearDownTest(c *gocheck.C) {
	newRemoteNode = t.oldNewRemoteNode
}

// tests that all peers are discovered on startup
func (t *PeerDiscoveryTest) TestPeerDiscoveryOnStartup(c *gocheck.C) {
	// TODO: this
	c.Skip("TODO: this")
}

// tests that the discoverPeers function returns the proper data
func (t *PeerDiscoveryTest) TestPeerDiscoveryResponse(c *gocheck.C) {
	// TODO: this
	c.Skip("TODO: this")
}

// sets up seeded peer discovery with a map of addresses -> connection accepted responses
// returns a cluster
func (t *PeerDiscoveryTest) setupSeedPeerDiscovery(c *gocheck.C, responses map[string]*ConnectionAcceptedResponse) *Cluster {
	seeds := make([]string, 0, len(responses))
	for k := range responses {
		seeds = append(seeds, k)
	}

	token := Token([]byte{0,0,1,0})
	cluster, err := NewCluster(
		kvstore.NewKVStore(),
		"127.0.0.1:9999",
		"TestCluster",
		token,
		node.NewNodeId(),
		DatacenterId("DC5000"),
		3,
		NewMD5Partitioner(),
		seeds,
	)
	c.Assert(err, gocheck.IsNil)

	// mock out remote node constructor
	newRemoteNode = func(addr string, clstr *Cluster) (*RemoteNode) {
		n := t.oldNewRemoteNode(addr, clstr)
		sock := newPgmConn()
		if response, exists := responses[addr]; !exists {
			c.Fatalf("Unexpected address: %v", addr)
		} else {
			sock.addOutgoingMessage(response)
		}
		sock.addOutgoingMessage(&DiscoverPeerResponse{})
		n.pool.Put(&Connection{socket:sock})
		return n
	}

	return cluster
}
func (t *PeerDiscoveryTest) compareNodeToConnectionResponse(c *gocheck.C, cluster *Cluster, n ClusterNode, addr string, response *ConnectionAcceptedResponse){
	c.Check(n.GetId(), gocheck.Equals, response.NodeId)
	c.Check(n.GetDatacenterId(), gocheck.Equals, response.DCId)
	c.Check(n.Name(), gocheck.Equals, response.Name)
	c.Check(n.GetAddr(), gocheck.Equals, addr)
	c.Check(n.GetStatus(), gocheck.Equals, NODE_UP)
	c.Check(n.GetToken(), gocheck.DeepEquals, response.Token)
}

// tests that discovering peers from a list of seed addresses
// works properly
func (t *PeerDiscoveryTest) TestDiscoveryFromSeedAddresses(c *gocheck.C) {
	// mocked out connections responses
	n2Response := &ConnectionAcceptedResponse{
		NodeId:node.NewNodeId(),
		DCId:DatacenterId("DC5000"),
		Name:"N2",
		Token:Token([]byte{0,0,2,0}),
	}
	n3Response  := &ConnectionAcceptedResponse{
		NodeId:node.NewNodeId(),
		DCId:DatacenterId("DC5000"),
		Name:"N3",
		Token:Token([]byte{0,0,3,0}),
	}
	responses := map[string]*ConnectionAcceptedResponse{
		"127.0.0.2:9999": n2Response,
		"127.0.0.3:9999": n3Response,
	}

	cluster := t.setupSeedPeerDiscovery(c, responses)
	err := cluster.discoverPeers()
	c.Assert(err, gocheck.IsNil)


	getNode := func(nid node.NodeId) ClusterNode {
		n, err := cluster.ring.GetNode(nid)
		c.Assert(err, gocheck.IsNil)
		return n
	}
	n2 := getNode(n2Response.NodeId)
	n3 := getNode(n3Response.NodeId)
	t.compareNodeToConnectionResponse(c, cluster, n2, "127.0.0.2:9999", n2Response)
	t.compareNodeToConnectionResponse(c, cluster, n3, "127.0.0.3:9999", n3Response)
}

// tests that discovering peers from a list of seed addresses
// that belong to different DCs works properly
func (t *PeerDiscoveryTest) TestOtherDCPeerDiscoveryFromSeedAddresses(c *gocheck.C) {
	// mocked out connections responses
	n2Response := &ConnectionAcceptedResponse{
		NodeId:node.NewNodeId(),
		DCId:DatacenterId("DC4000"),
		Name:"N2",
		Token:Token([]byte{0,0,2,0}),
	}
	n3Response  := &ConnectionAcceptedResponse{
		NodeId:node.NewNodeId(),
		DCId:DatacenterId("DC4000"),
		Name:"N3",
		Token:Token([]byte{0,0,3,0}),
	}
	responses := map[string]*ConnectionAcceptedResponse{
		"127.0.0.2:9999": n2Response,
		"127.0.0.3:9999": n3Response,
	}

	cluster := t.setupSeedPeerDiscovery(c, responses)
	err := cluster.discoverPeers()
	c.Assert(err, gocheck.IsNil)

	getNode := func(nid node.NodeId) ClusterNode {
		ring, err := cluster.dcContainer.GetRing("DC4000")
		c.Assert(err, gocheck.IsNil)
		n, err := ring.getNode(nid)
		c.Assert(err, gocheck.IsNil)
		return n
	}

	n2 := getNode(n2Response.NodeId)
	n3 := getNode(n3Response.NodeId)
	t.compareNodeToConnectionResponse(c, cluster, n2, "127.0.0.2:9999", n2Response)
	t.compareNodeToConnectionResponse(c, cluster, n3, "127.0.0.3:9999", n3Response)
}

func (t *PeerDiscoveryTest) setupDiscoverFromExistingPeers(c *gocheck.C, responses map[string]*ConnectionAcceptedResponse) *Cluster {
	token := Token([]byte{0,0,0,0})
	cluster, err := NewCluster(
		kvstore.NewKVStore(),
		"127.0.0.0:9999",
		"TestCluster",
		token,
		node.NewNodeId(),
		DatacenterId("DC5000"),
		3,
		NewMD5Partitioner(),
		nil,
	)
	c.Assert(err, gocheck.IsNil)

	// create existing remote node
	rnode := NewRemoteNodeInfo(
		node.NewNodeId(),
		DatacenterId("DC5000"),
		Token([]byte{0,0,1,0}),
		"N1",
		"127.0.0.1:9999",
		cluster,
	)

	peerData := make([]*PeerData, 0, len(responses))
	for addr, response := range responses {
		data := &PeerData{
			NodeId:response.NodeId,
			DCId:response.DCId,
			Name:response.Name,
			Token:response.Token,
			Addr:addr,
		}
		peerData = append(peerData, data)
	}
	discoveryResponse := &DiscoverPeerResponse{Peers:peerData}

	// mock out existing node
	sock := newPgmConn()
	sock.addOutgoingMessage(discoveryResponse)
	conn := &Connection{socket:sock}
	conn.SetHandshakeCompleted()
	rnode.pool.Put(conn)

	// add to cluster
	err = cluster.addNode(rnode)
	c.Assert(err, gocheck.IsNil)

	// mock out remote node constructor
	newRemoteNode = func(addr string, clstr *Cluster) (*RemoteNode) {
		n := t.oldNewRemoteNode(addr, clstr)
		sock := newPgmConn()
		response, exists := responses[addr]
		c.Assert(exists, gocheck.Equals, true)
		sock.addOutgoingMessage(response)

		sock.addOutgoingMessage(&DiscoverPeerResponse{})
		n.pool.Put(&Connection{socket:sock})
		return n
	}
	return cluster
}

// tests that discovering peers from existing peers
// works properly
func (t *PeerDiscoveryTest) TestDiscoveryFromExistingPeers(c *gocheck.C) {
	// mocked out responses
	n2Response := &ConnectionAcceptedResponse{
		NodeId:node.NewNodeId(),
		DCId:DatacenterId("DC5000"),
		Name:"N2",
		Token:Token([]byte{0,0,2,0}),
	}
	n3Response := &ConnectionAcceptedResponse{
		NodeId:node.NewNodeId(),
		DCId:DatacenterId("DC5000"),
		Name:"N3",
		Token:Token([]byte{0,0,3,0}),
	}
	responses := map[string]*ConnectionAcceptedResponse{
		"127.0.0.2:9999": n2Response,
		"127.0.0.3:9999": n3Response,
	}

	cluster := t.setupDiscoverFromExistingPeers(c, responses)

	err := cluster.discoverPeers()
	c.Assert(err, gocheck.IsNil)

	getNode := func(nid node.NodeId) ClusterNode {
		n, err := cluster.ring.GetNode(nid)
		c.Assert(err, gocheck.IsNil)
		return n
	}
	n2 := getNode(n2Response.NodeId)
	n3 := getNode(n3Response.NodeId)

	n2.Start()
	n3.Start()
	t.compareNodeToConnectionResponse(c, cluster, n2, "127.0.0.2:9999", n2Response)
	t.compareNodeToConnectionResponse(c, cluster, n3, "127.0.0.3:9999", n3Response)
}

func (t *PeerDiscoveryTest) TestOtherDCDiscoveryFromExistingPeers(c *gocheck.C) {
	// mocked out responses
	n2Response := &ConnectionAcceptedResponse{
		NodeId:node.NewNodeId(),
		DCId:DatacenterId("DC4000"),
		Name:"N2",
		Token:Token([]byte{0,0,2,0}),
	}
	n3Response := &ConnectionAcceptedResponse{
		NodeId:node.NewNodeId(),
		DCId:DatacenterId("DC4000"),
		Name:"N3",
		Token:Token([]byte{0,0,3,0}),
	}
	responses := map[string]*ConnectionAcceptedResponse{
		"127.0.0.2:9999": n2Response,
		"127.0.0.3:9999": n3Response,
	}

	cluster := t.setupDiscoverFromExistingPeers(c, responses)

	err := cluster.discoverPeers()
	c.Assert(err, gocheck.IsNil)

	getNode := func(nid node.NodeId) ClusterNode {
		ring, err := cluster.dcContainer.GetRing("DC4000")
		c.Assert(err, gocheck.IsNil)
		n, err := ring.getNode(nid)
		c.Assert(err, gocheck.IsNil)
		return n
	}
	n2 := getNode(n2Response.NodeId)
	n3 := getNode(n3Response.NodeId)

	n2.Start()
	n3.Start()
	t.compareNodeToConnectionResponse(c, cluster, n2, "127.0.0.2:9999", n2Response)
	t.compareNodeToConnectionResponse(c, cluster, n3, "127.0.0.3:9999", n3Response)
}

func (t *PeerDiscoveryTest) TestPeerDiscoverySeedFailure(c *gocheck.C) {
	seeds := []string{"127.0.0.2:9999", "127.0.0.3:9999"}

	token := Token([]byte{0,0,1,0})
	cluster, err := NewCluster(
		kvstore.NewKVStore(),
		"127.0.0.1:9999",
		"TestCluster",
		token,
		node.NewNodeId(),
		DatacenterId("DC1234"),
		3,
		NewMD5Partitioner(),
		seeds,
	)
	c.Assert(err, gocheck.IsNil)

	err = cluster.discoverPeers()
	c.Assert(err, gocheck.IsNil)

	c.Check(len(cluster.ring.tokenRing), gocheck.Equals, 1)
}

// tests that a node is still added to the ring, even if
// there's a problem connecting to it when discovered from
// another node
func (t *PeerDiscoveryTest) TestPeerDiscoveryNodeDataFailure(c *gocheck.C) {
	token := Token([]byte{0,0,0,0})
	cluster, err := NewCluster(
		kvstore.NewKVStore(),
		"127.0.0.0:9999",
		"TestCluster",
		token,
		node.NewNodeId(),
		DatacenterId("DC1234"),
		3,
		NewMD5Partitioner(),
		nil,
	)
	c.Assert(err, gocheck.IsNil)

	// create existing remote node
	rnode := NewRemoteNodeInfo(
		node.NewNodeId(),
		DatacenterId("DC1234"),
		Token([]byte{0,0,1,0}),
		"N1",
		"127.0.0.1:9999",
		cluster,
	)

	// mocked out responses
	n2Response := &ConnectionAcceptedResponse{
		NodeId:node.NewNodeId(),
		Name:"N2",
		Token:Token([]byte{0,0,2,0}),
	}
	n3Response := &ConnectionAcceptedResponse{
		NodeId:node.NewNodeId(),
		Name:"N3",
		Token:Token([]byte{0,0,3,0}),
	}
	discoveryResponse := &DiscoverPeerResponse{Peers:[]*PeerData{
		&PeerData{
			NodeId:n2Response.NodeId,
			DCId:DatacenterId("DC1234"),
			Name:n2Response.Name,
			Token:n2Response.Token,
			Addr:"127.0.0.2:9999",
		},
		&PeerData{
			NodeId:n3Response.NodeId,
			DCId:DatacenterId("DC1234"),
			Name:n3Response.Name,
			Token:n3Response.Token,
			Addr:"127.0.0.3:9999",
		},
	}}

	// mock out existing node
	sock := newBiConn(2, 2)
	message.WriteMessage(sock.input[0], discoveryResponse)
	conn := &Connection{socket:sock}
	conn.SetHandshakeCompleted()
	rnode.pool.Put(conn)

	// add to cluster
	err = cluster.addNode(rnode)
	c.Assert(err, gocheck.IsNil)

	// mock out remote node constructor
	newRemoteNode = func(addr string, clstr *Cluster) (*RemoteNode) {
		n := t.oldNewRemoteNode(addr, clstr)
		var response *ConnectionAcceptedResponse
		sock := newBiConn(2, 2)
		switch addr {
		case "127.0.0.2:9999":
			response = n2Response
		case "127.0.0.3:9999":
			// timeout!
			conn := &Connection{socket:&timeoutConn{}}
			n.pool.Put(conn)
			return n
		default:
			panic(fmt.Sprintf("Unexpected address: %v", addr))
		}
		message.WriteMessage(sock.input[0], response)
		discResp := &DiscoverPeerResponse{}
		message.WriteMessage(sock.input[1], discResp)
		conn := &Connection{socket:sock}
		n.pool.Put(conn)
		return n
	}

	err = cluster.discoverPeers()
	c.Assert(err, gocheck.IsNil)

	n2, err := cluster.ring.GetNode(n2Response.NodeId)
	c.Assert(err, gocheck.IsNil)
	n3, err := cluster.ring.GetNode(n3Response.NodeId)
	c.Assert(err, gocheck.IsNil)

	n2.Start()
	n3.Start()

	t.compareNodeToConnectionResponse(c, cluster, n2, "127.0.0.2:9999", n2Response)

	// we're checking that the node is down, so we can't use compareNodeToConnectionResponse
	c.Check(n3.GetId(), gocheck.Equals, n3Response.NodeId)
	c.Check(n3.Name(), gocheck.Equals, n3Response.Name)
	c.Check(n3.GetAddr(), gocheck.Equals, "127.0.0.3:9999")
	c.Check(n3.GetStatus(), gocheck.Equals, NODE_DOWN)
	c.Check(n3.GetToken(), gocheck.DeepEquals, n3Response.Token)
}

/************** shutdown tests **************/

// TODO: this
type ShutdownTest struct {

}

var _ = gocheck.Suite(&ShutdownTest{})


