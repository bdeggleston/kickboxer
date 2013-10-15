/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/8/13
 * Time: 9:39 PM
 * To change this template use File | Settings | File Templates.
 */
package cluster

import (
	"fmt"
	"testing"
)


func setupCluster() *Cluster {
	c, err := NewCluster(
		"127.0.0.1:9999",
		"Test Cluster",
		Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
		NewNodeId(),
		3,
		NewMD5Partitioner(),
		nil,
	)
	if err != nil {
		panic(fmt.Sprintf("Unexpected error instantiating cluster: %v", err))
	}
	return c
}
// tests the cluster constructor works as expected
// and all of it's basic methods return the proper
// values
func TestClusterSetup(t *testing.T) {
	cluster := setupCluster()
	equalityCheck(t, "cluster name", cluster.name, cluster.GetName())
	equalityCheck(t, "cluster nodeId", cluster.nodeId, cluster.GetNodeId())
	equalityCheck(t, "cluster addr", cluster.peerAddr, cluster.GetPeerAddr())
	sliceEqualityCheck(t, "cluster name", cluster.token, cluster.GetToken())
}

// tests that instantiating a cluster with an invalid replication
// factor returns an error
func TestInvalidReplicationFactor(t *testing.T) {
	c, err := NewCluster(
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

/************** key routing tests **************/

// makes a ring of the given size, with the tokens evenly spaced
func makeRing(size int, replicationFactor uint32) *Cluster {
	c, err := NewCluster(
		"127.0.0.1:9999",
		"Test Cluster",
		Token([]byte{0,0,0,0}),
		NewNodeId(),
		replicationFactor,
		NewMD5Partitioner(),
		nil,
	)
	if err != nil {
		panic(fmt.Sprintf("Unexpected error instantiating cluster: %v", err))
	}

	for i:=1; i<size; i++ {
		n := newMockNode(
			NewNodeId(),
			Token([]byte{0,0,byte(i),0}),
			fmt.Sprintf("N%v", i),
		)
		c.addNode(n)
	}

	return c
}

// tests that the number of nodes returned matches the replication factor
func TestReplicationFactor(t *testing.T) {

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

// tests that discovering peers from a list of seed addresses
// works properly
func TestPeerDiscoveryFromSeedAddresses(t *testing.T) {

}

// tests that discovering peers from existing peers
// works properly
func TestPeerDiscoveryFromExistingPeers(t *testing.T) {

}

// tests that a node is skipped if it can't be connected
// to from the seed list
func TestPeerDiscoverySeedFailure(t *testing.T) {

}

// tests that a node is still added to the ring, even if
// there's a problem connecting to it when discovered from
// another node
func TestPeerDiscoveryNodeDataFailure(t *testing.T) {

}

/************** shutdown tests **************/

/************** query tests **************/



