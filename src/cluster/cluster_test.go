package cluster

import (
	"fmt"
	"testing"
	"testing_helpers"
)

import (
	"kvstore"
)

var (
	originalNewRemoteNode = newRemoteNode
)

func tearDownNewRemoteNode() {
	newRemoteNode = originalNewRemoteNode
}
// tests the cluster constructor works as expected
// and all of it's basic methods return the proper
// values
func TestClusterSetup(t *testing.T) {
	cluster := setupCluster()
	testing_helpers.AssertEqual(t, "cluster name", cluster.name, cluster.GetName())
	testing_helpers.AssertEqual(t, "cluster nodeId", cluster.nodeId, cluster.GetNodeId())
	testing_helpers.AssertEqual(t, "cluster addr", cluster.peerAddr, cluster.GetPeerAddr())
	testing_helpers.AssertSliceEqual(t, "cluster name", cluster.token, cluster.GetToken())
}

// tests that instantiating a cluster with an invalid replication
// factor returns an error
func TestInvalidReplicationFactor(t *testing.T) {
	c, err := NewCluster(
		kvstore.NewKVStore(),
		"127.0.0.1:9999",
		"Test Cluster",
		Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
		NewNodeId(),
		0,
		NewMD5Partitioner(),
		nil,
	)

	if c != nil {
		t.Error("unexpected non nil cluster")
	}

	if err == nil {
		t.Error("expected error from cluster constructor, got nil")
	}
}

func TestInvalidPartitioner(t *testing.T) {
	c, err := NewCluster(
		kvstore.NewKVStore(),
		"127.0.0.1:9999",
		"Test Cluster",
		Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
		NewNodeId(),
		3,
		nil,
		nil,
	)

	if c != nil {
		t.Error("unexpected non nil cluster")
	}

	if err == nil {
		t.Error("expected error from cluster constructor, got nil")
	}

}

/************** addNode tests **************/

// tests that a node is added to the cluster if
// the cluster has not seen it yet, and starts it
// if the cluster has been started
func TestAddingNewNodeToStartedCluster(t *testing.T) {
	t.Skip("Cluster starting not implemented yet")
}

/************** getPeerData tests **************/

func TestExpectedPeerDataIsReturned(t *testing.T) {
	c := makeRing(5, 3)

	var data []*PeerData

	data = c.getPeerData()
	for i, pd := range data {
		node, err := c.ring.GetNode(pd.NodeId)
		if err != nil {
			t.Errorf("Unexpected error returned for: %v", pd.NodeId)
		}
		if node == nil {
			t.Errorf("Unexpected nil node returned for: %v", pd.NodeId)
		}
		testing_helpers.AssertEqual(t, fmt.Sprintf("n[%v] Name", i), node.Name(), pd.Name)
		testing_helpers.AssertEqual(t, fmt.Sprintf("n[%v] Addr", i), node.GetAddr(), pd.Addr)
		testing_helpers.AssertSliceEqual(t, fmt.Sprintf("n[%v] Token", i), node.GetToken(), pd.Token)
	}
}

func TestSelfNodeIsNotReturned(t *testing.T) {
	c := makeRing(5, 3)

	var data []*PeerData

	data = c.getPeerData()
	testing_helpers.AssertEqual(t, "data size", 4, len(data))
	for _, pd := range data {
		if pd.NodeId == c.GetNodeId() {
			t.Errorf("local node found in peer data")
		}
	}
}

/************** key routing tests **************/

// tests that the number of nodes returned matches the replication factor
func TestReplicationFactor(t *testing.T) {
	t.Skipf("check number of nodes returned matches replication factor")
}

/************** startup tests **************/

func TestNodesAreStartedOnStartup(t *testing.T) {
	c := makeRing(10, 3)
	err := c.Start()
	defer c.Stop()
	if err != nil {
		t.Errorf("Unexpected error starting cluster: %v", err)
	}

	for i, n := range c.ring.AllNodes() {
		if !n.IsStarted() {
			t.Errorf("Unexpected non-started node at token ring index %v", i)
		}
	}

}

func TestClusterStatusIsChangedOnStartup(t *testing.T) {
	c := makeRing(10, 3)
	if c.status != CLUSTER_INITIALIZING {
		t.Fatalf("Unexpected initial cluster status. Expected %v, got %v", CLUSTER_INITIALIZING, c.status)
	}
	err := c.Start()
	defer c.Stop()
	if err != nil {
		t.Errorf("Unexpected error starting cluster: %v", err)
	}
	if c.status != CLUSTER_NORMAL {
		t.Fatalf("Unexpected initial cluster status. Expected %v, got %v", CLUSTER_NORMAL, c.status)
	}
}

func TestPeerServerIsStartedOnStartup(t *testing.T) {
	c := makeRing(10, 3)
	if c.peerServer.isRunning {
		t.Fatal("PeerServer unexpectedly running before cluster start")
	}
	err := c.Start()
	defer c.Stop()
	if err != nil {
		t.Errorf("Unexpected error starting cluster: %v", err)
	}
	if !c.peerServer.isRunning {
		t.Fatal("PeerServer unexpectedly not running after cluster start")
	}

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

// tests that all peers are discovered on startup
func TestPeerDiscoveryOnStartup(t *testing.T) {

}

// tests that the discoverPeers function returns the proper data
func TestPeerDiscoveryResponse(t *testing.T) {

}

// tests that discovering peers from a list of seed addresses
// works properly
func TestPeerDiscoveryFromSeedAddresses(t *testing.T) {
	defer tearDownNewRemoteNode()

	seeds := []string{"127.0.0.2:9999", "127.0.0.3:9999"}

	token := Token([]byte{0,0,1,0})
	cluster, err := NewCluster(
		kvstore.NewKVStore(),
		"127.0.0.1:9999",
		"TestCluster",
		token,
		NewNodeId(),
		3,
		NewMD5Partitioner(),
		seeds,
	)

	if err != nil {
		t.Fatalf("Unexpected error in cluster creation: %v", err)
	}

	// mocked out connections responses
	n2Response := &ConnectionAcceptedResponse{
		NodeId:NewNodeId(),
		Name:"N2",
		Token:Token([]byte{0,0,2,0}),
	}
	n3Response := &ConnectionAcceptedResponse{
		NodeId:NewNodeId(),
		Name:"N3",
		Token:Token([]byte{0,0,3,0}),
	}

	// mock out remote node constructor
	newRemoteNode = func(addr string, clstr *Cluster) (*RemoteNode) {
		node := originalNewRemoteNode(addr, clstr)
		var response *ConnectionAcceptedResponse
		sock := newBiConn(2, 2)
		switch addr {
		case "127.0.0.2:9999":
			response = n2Response
		case "127.0.0.3:9999":
			response = n3Response
		default:
			panic(fmt.Sprintf("Unexpected address: %v", addr))
		}
		WriteMessage(sock.input[0], response)
		discResp := &DiscoverPeerResponse{}
		WriteMessage(sock.input[1], discResp)
		conn := &Connection{socket:sock}
		node.pool.Put(conn)
		return node
	}

	if err := cluster.discoverPeers(); err != nil {
		t.Fatalf("Unexpected error discovering peers: %v", err)
	}

	n2, err := cluster.ring.GetNode(n2Response.NodeId)
	n3, err := cluster.ring.GetNode(n3Response.NodeId)
	if err != nil { t.Fatalf("n2 was not found: %v", err) }
	if err != nil { t.Fatalf("n3 was not found: %v", err) }

	testing_helpers.AssertEqual(t, "n2 id", n2.GetId(), n2Response.NodeId)
	testing_helpers.AssertEqual(t, "n2 name", n2.Name(), n2Response.Name)
	testing_helpers.AssertEqual(t, "n2 addr", n2.GetAddr(), "127.0.0.2:9999")
	testing_helpers.AssertEqual(t, "n2 status", NODE_UP, n2.GetStatus())
	testing_helpers.AssertSliceEqual(t, "n2 token", n2.GetToken(), n2Response.Token)

	testing_helpers.AssertEqual(t, "n3 id", n3.GetId(), n3Response.NodeId)
	testing_helpers.AssertEqual(t, "n3 name", n3.Name(), n3Response.Name)
	testing_helpers.AssertEqual(t, "n3 addr", n3.GetAddr(), "127.0.0.3:9999")
	testing_helpers.AssertEqual(t, "n3 status", NODE_UP, n3.GetStatus())
	testing_helpers.AssertSliceEqual(t, "n3 token", n3.GetToken(), n3Response.Token)
}

// tests that discovering peers from existing peers
// works properly
func TestPeerDiscoveryFromExistingPeers(t *testing.T) {
	defer tearDownNewRemoteNode()

	token := Token([]byte{0,0,0,0})
	cluster, err := NewCluster(
		kvstore.NewKVStore(),
		"127.0.0.0:9999",
		"TestCluster",
		token,
		NewNodeId(),
		3,
		NewMD5Partitioner(),
		nil,
	)
	if err != nil {
		t.Fatalf("Unexpected error in cluster creation: %v", err)
	}

	// create existing remote node
	rnode := NewRemoteNodeInfo(NewNodeId(), Token([]byte{0,0,1,0}), "N1", "127.0.0.1:9999", cluster)

	// mocked out responses
	n2Response := &ConnectionAcceptedResponse{
		NodeId:NewNodeId(),
		Name:"N2",
		Token:Token([]byte{0,0,2,0}),
	}
	n3Response := &ConnectionAcceptedResponse{
		NodeId:NewNodeId(),
		Name:"N3",
		Token:Token([]byte{0,0,3,0}),
	}
	discoveryResponse := &DiscoverPeerResponse{Peers:[]*PeerData{
		&PeerData{
			NodeId:n2Response.NodeId,
			Name:n2Response.Name,
			Token:n2Response.Token,
			Addr:"127.0.0.2:9999",
		},
		&PeerData{
			NodeId:n3Response.NodeId,
			Name:n3Response.Name,
			Token:n3Response.Token,
			Addr:"127.0.0.3:9999",
		},
	}}

	// mock out existing node
	sock := newBiConn(2, 2)
	WriteMessage(sock.input[0], discoveryResponse)
	conn := &Connection{socket:sock}
	conn.SetHandshakeCompleted()
	rnode.pool.Put(conn)

	// add to cluster
	if err := cluster.addNode(rnode); err != nil {
		t.Fatalf("Unexpected error adding node to cluster: %v", err)
	}

	// mock out remote node constructor
	newRemoteNode = func(addr string, clstr *Cluster) (*RemoteNode) {
		node := originalNewRemoteNode(addr, clstr)
		var response *ConnectionAcceptedResponse
		sock := newBiConn(2, 2)
		switch addr {
		case "127.0.0.2:9999":
			response = n2Response
		case "127.0.0.3:9999":
			response = n3Response
		default:
			panic(fmt.Sprintf("Unexpected address: %v", addr))
		}
		WriteMessage(sock.input[0], response)
		discResp := &DiscoverPeerResponse{}
		WriteMessage(sock.input[1], discResp)
		conn := &Connection{socket:sock}
		node.pool.Put(conn)
		return node
	}

	if err := cluster.discoverPeers(); err != nil {
		t.Fatalf("Unexpected error discovering peers: %v", err)
	}

	n2, err := cluster.ring.GetNode(n2Response.NodeId)
	n3, err := cluster.ring.GetNode(n3Response.NodeId)
	if err != nil { t.Fatalf("n2 was not found: %v", err) }
	if err != nil { t.Fatalf("n3 was not found: %v", err) }

	n2.Start()
	n3.Start()

	testing_helpers.AssertEqual(t, "n2 id", n2.GetId(), n2Response.NodeId)
	testing_helpers.AssertEqual(t, "n2 name", n2.Name(), n2Response.Name)
	testing_helpers.AssertEqual(t, "n2 addr", n2.GetAddr(), "127.0.0.2:9999")
	testing_helpers.AssertEqual(t, "n2 status", NODE_UP, n2.GetStatus())
	testing_helpers.AssertSliceEqual(t, "n2 token", n2.GetToken(), n2Response.Token)

	testing_helpers.AssertEqual(t, "n3 id", n3.GetId(), n3Response.NodeId)
	testing_helpers.AssertEqual(t, "n3 name", n3.Name(), n3Response.Name)
	testing_helpers.AssertEqual(t, "n3 addr", n3.GetAddr(), "127.0.0.3:9999")
	testing_helpers.AssertEqual(t, "n3 status", NODE_UP, n3.GetStatus())
	testing_helpers.AssertSliceEqual(t, "n3 token", n3.GetToken(), n3Response.Token)
}

// tests that a node is skipped if it can't be connected
// to from the seed list
func TestPeerDiscoverySeedFailure(t *testing.T) {
	defer tearDownNewRemoteNode()

	seeds := []string{"127.0.0.2:9999", "127.0.0.3:9999"}

	token := Token([]byte{0,0,1,0})
	cluster, err := NewCluster(kvstore.NewKVStore(), "127.0.0.1:9999", "TestCluster", token, NewNodeId(), 3, NewMD5Partitioner(), seeds)
	if err != nil {
		t.Fatalf("Unexpected error in cluster creation: %v", err)
	}

	if err := cluster.discoverPeers(); err != nil {
		t.Fatalf("Unexpected error discovering peers: %v", err)
	}

	if len(cluster.ring.tokenRing) != 1 {
		t.Fatalf("Unexpected ring size. Expecting 1, got: %v", len(cluster.ring.tokenRing))
	}

}

// tests that a node is still added to the ring, even if
// there's a problem connecting to it when discovered from
// another node
func TestPeerDiscoveryNodeDataFailure(t *testing.T) {
	defer tearDownNewRemoteNode()

	token := Token([]byte{0,0,0,0})
	cluster, err := NewCluster(kvstore.NewKVStore(), "127.0.0.0:9999", "TestCluster", token, NewNodeId(), 3, NewMD5Partitioner(), nil)
	if err != nil {
		t.Fatalf("Unexpected error in cluster creation: %v", err)
	}

	// create existing remote node
	rnode := NewRemoteNodeInfo(NewNodeId(), Token([]byte{0,0,1,0}), "N1", "127.0.0.1:9999", cluster)

	// mocked out responses
	n2Response := &ConnectionAcceptedResponse{
		NodeId:NewNodeId(),
		Name:"N2",
		Token:Token([]byte{0,0,2,0}),
	}
	n3Response := &ConnectionAcceptedResponse{
		NodeId:NewNodeId(),
		Name:"N3",
		Token:Token([]byte{0,0,3,0}),
	}
	discoveryResponse := &DiscoverPeerResponse{Peers:[]*PeerData{
		&PeerData{
			NodeId:n2Response.NodeId,
			Name:n2Response.Name,
			Token:n2Response.Token,
			Addr:"127.0.0.2:9999",
		},
		&PeerData{
			NodeId:n3Response.NodeId,
			Name:n3Response.Name,
			Token:n3Response.Token,
			Addr:"127.0.0.3:9999",
		},
	}}

	// mock out existing node
	sock := newBiConn(2, 2)
	WriteMessage(sock.input[0], discoveryResponse)
	conn := &Connection{socket:sock}
	conn.SetHandshakeCompleted()
	rnode.pool.Put(conn)

	// add to cluster
	if err := cluster.addNode(rnode); err != nil {
		t.Fatalf("Unexpected error adding node to cluster: %v", err)
	}

	// mock out remote node constructor
	newRemoteNode = func(addr string, clstr *Cluster) (*RemoteNode) {
		node := originalNewRemoteNode(addr, clstr)
		var response *ConnectionAcceptedResponse
		sock := newBiConn(2, 2)
		switch addr {
		case "127.0.0.2:9999":
			response = n2Response
		case "127.0.0.3:9999":
			// timeout!
			conn := &Connection{socket:&timeoutConn{}}
			node.pool.Put(conn)
			return node
		default:
			panic(fmt.Sprintf("Unexpected address: %v", addr))
		}
		WriteMessage(sock.input[0], response)
		discResp := &DiscoverPeerResponse{}
		WriteMessage(sock.input[1], discResp)
		conn := &Connection{socket:sock}
		node.pool.Put(conn)
		return node
	}

	if err := cluster.discoverPeers(); err != nil {
		t.Fatalf("Unexpected error discovering peers: %v", err)
	}

	n2, err := cluster.ring.GetNode(n2Response.NodeId)
	n3, err := cluster.ring.GetNode(n3Response.NodeId)
	if err != nil { t.Fatalf("n2 was not found: %v", err) }
	if err != nil { t.Fatalf("n3 was not found: %v", err) }

	n2.Start()
	n3.Start()

	testing_helpers.AssertEqual(t, "n2 id", n2.GetId(), n2Response.NodeId)
	testing_helpers.AssertEqual(t, "n2 name", n2.Name(), n2Response.Name)
	testing_helpers.AssertEqual(t, "n2 addr", n2.GetAddr(), "127.0.0.2:9999")
	testing_helpers.AssertEqual(t, "n2 status", NODE_UP, n2.GetStatus())
	testing_helpers.AssertSliceEqual(t, "n2 token", n2.GetToken(), n2Response.Token)

	testing_helpers.AssertEqual(t, "n3 id", n3.GetId(), n3Response.NodeId)
	testing_helpers.AssertEqual(t, "n3 name", n3.Name(), n3Response.Name)
	testing_helpers.AssertEqual(t, "n3 addr", n3.GetAddr(), "127.0.0.3:9999")
	testing_helpers.AssertEqual(t, "n3 status", NODE_DOWN, n3.GetStatus())
	testing_helpers.AssertSliceEqual(t, "n3 token", n3.GetToken(), n3Response.Token)

}

/************** shutdown tests **************/

/************** query tests **************/



