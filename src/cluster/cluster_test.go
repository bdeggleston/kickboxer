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
	)

	if c != nil {
		t.Error("unexpected non nil cluster")
	}

	if err == nil {
		t.Error("expected error from cluster constructor, got nil")
	}

}

/************** getNode tests **************/


// tests that calling getNode with a non-existant
// node id returns an error
func TestGetUnknownNode(t *testing.T) {
	cluster := setupCluster()
	node, err := cluster.getNode(NewNodeId())
	if node != nil {
		t.Errorf("Expected nil node, got: %v", node)
	}
	if err == nil {
		t.Errorf("Expected error from getNode, got nil")
	}
}

// tests that fetching a node with a valid node id
// returns the requested node
func TestGetExistingNode(t *testing.T) {
	cluster := setupCluster()
	nid := cluster.GetNodeId()
	node, err := cluster.getNode(nid)
	if err != nil {
		t.Errorf("Got unexpected error from getNode: %v", err)
	}
	if node == nil {
		t.Fatalf("Got unexpected nil result for node")
	}
	if node.GetId() != nid {
		t.Errorf("Unexpected node id on returned node. Expected %v, got %v", nid, node.GetId())
	}
}

/************** addNode tests **************/

// tests that a node is added to the cluster if
// the cluster has not seen it yet
func TestAddingNewNodeToStoppedCluster(t *testing.T) {
	cluster := setupCluster()

	// sanity check
	equalityCheck(t, "cluster map size", 1, len(cluster.nodeMap))
	equalityCheck(t, "cluster ring size", 1, len(cluster.tokenRing))

	// add a new node
	token := Token([]byte{0,0,1,2,3,4,5,6,7,0,1,2,3,4,5,6})
	newNode := newMockNode(NewNodeId(), token, "N2")
	cluster.addNode(newNode)

	equalityCheck(t, "cluster map size", 2, len(cluster.nodeMap))
	equalityCheck(t, "cluster ring size", 2, len(cluster.tokenRing))
	equalityCheck(t, "new node started", false, newNode.IsStarted())
	equalityCheck(t, "new node read size", 0, len(newNode.reads))
	equalityCheck(t, "new node write size", 0, len(newNode.writes))
}

// tests that a node is added to the cluster if
// the cluster has not seen it yet, and starts it
// if the cluster has been started
func TestAddingNewNodeToStartedCluster(t *testing.T) {
	t.Skip("Cluster starting not implemented yet")
}

// tests that nothing is changed if a node is already
// known by the cluster
func TestAddingExistingNodeToCluster(t *testing.T) {

	cluster := setupCluster()

	// sanity check
	equalityCheck(t, "cluster map size", 1, len(cluster.nodeMap))
	equalityCheck(t, "cluster ring size", 1, len(cluster.tokenRing))

	cluster.addNode(cluster.localNode)

	equalityCheck(t, "cluster map size", 1, len(cluster.nodeMap))
	equalityCheck(t, "cluster ring size", 1, len(cluster.tokenRing))
}

/************** refreshRing tests **************/

func TestRingIsRefreshedAfterNodeAddition(t *testing.T) {
	c, _ := NewCluster(
		"127.0.0.1:9999",
		"Test Cluster",
		Token([]byte{0,0,0,7}),
		NewNodeId(),
		3,
		NewMD5Partitioner(),
	)
	n1 := c.localNode

	n2 := newMockNode(
		NewNodeId(),
		Token([]byte{0,0,0,3}),
		"N2",
	)
	c.addNode(n2)

	equalityCheck(t, "ring position 0", c.tokenRing[0].GetId(), n2.GetId())
	equalityCheck(t, "ring position 1", c.tokenRing[1].GetId(), n1.GetId())

	n3 := newMockNode(
		NewNodeId(),
		Token([]byte{0,0,0,5}),
		"N3",
	)
	c.addNode(n3)

	equalityCheck(t, "ring position 0", c.tokenRing[0].GetId(), n2.GetId())
	equalityCheck(t, "ring position 1", c.tokenRing[1].GetId(), n3.GetId())
	equalityCheck(t, "ring position 2", c.tokenRing[2].GetId(), n1.GetId())

	n4 := newMockNode(
		NewNodeId(),
		Token([]byte{0,0,1,0}),
		"N4",
	)
	c.addNode(n4)

	equalityCheck(t, "ring position 0", c.tokenRing[0].GetId(), n2.GetId())
	equalityCheck(t, "ring position 1", c.tokenRing[1].GetId(), n3.GetId())
	equalityCheck(t, "ring position 2", c.tokenRing[2].GetId(), n1.GetId())
	equalityCheck(t, "ring position 3", c.tokenRing[3].GetId(), n4.GetId())

	for i:=0;i<len(c.tokenRing);i++ {
		n := c.tokenRing[i]
		t.Log(n.Name(), n.GetToken())
	}
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

// tests that the proper nodes are returned for the given keys
func TestKeyRouting(t *testing.T) {
	c := makeRing(10, 3)

	var token Token
	var nodes []Node

	// test the upper bound
	token = Token([]byte{0,0,9,5})
	nodes = c.GetNodesForToken(token)
	if len(nodes) != 3 { t.Fatalf("wrong number of nodes returned, expected 3, got %v", len(nodes)) }
	equalityCheck(t, "node[0]", c.tokenRing[0].GetId(), nodes[0].GetId())
	equalityCheck(t, "node[1]", c.tokenRing[1].GetId(), nodes[1].GetId())
	equalityCheck(t, "node[2]", c.tokenRing[2].GetId(), nodes[2].GetId())

	// test the lower bound
	token = Token([]byte{0,0,0,0})
	nodes = c.GetNodesForToken(token)
	if len(nodes) != 3 { t.Fatalf("wrong number of nodes returned, expected 3, got %v", len(nodes)) }
	equalityCheck(t, "node[0]", c.tokenRing[0].GetId(), nodes[0].GetId())
	equalityCheck(t, "node[1]", c.tokenRing[1].GetId(), nodes[1].GetId())
	equalityCheck(t, "node[2]", c.tokenRing[2].GetId(), nodes[2].GetId())

	// test token intersection
	token = Token([]byte{0,0,4,0})
	nodes = c.GetNodesForToken(token)
	if len(nodes) != 3 { t.Fatalf("wrong number of nodes returned, expected 3, got %v", len(nodes)) }
	equalityCheck(t, "node[0]", c.tokenRing[4].GetId(), nodes[0].GetId())
	equalityCheck(t, "node[1]", c.tokenRing[5].GetId(), nodes[1].GetId())
	equalityCheck(t, "node[2]", c.tokenRing[6].GetId(), nodes[2].GetId())

	// test middle range
	token = Token([]byte{0,0,4,5})
	nodes = c.GetNodesForToken(token)
	if len(nodes) != 3 { t.Fatalf("wrong number of nodes returned, expected 3, got %v", len(nodes)) }
	equalityCheck(t, "node[0]", c.tokenRing[5].GetId(), nodes[0].GetId())
	equalityCheck(t, "node[1]", c.tokenRing[6].GetId(), nodes[1].GetId())
	equalityCheck(t, "node[2]", c.tokenRing[7].GetId(), nodes[2].GetId())

	// test wrapping
	token = Token([]byte{0,0,8,5})
	nodes = c.GetNodesForToken(token)
	if len(nodes) != 3 { t.Fatalf("wrong number of nodes returned, expected 3, got %v", len(nodes)) }
	equalityCheck(t, "node[0]", c.tokenRing[9].GetId(), nodes[0].GetId())
	equalityCheck(t, "node[1]", c.tokenRing[0].GetId(), nodes[1].GetId())
	equalityCheck(t, "node[2]", c.tokenRing[1].GetId(), nodes[2].GetId())
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

	for i:=0; i<len(c.tokenRing); i++ {
		n := c.tokenRing[i]
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

// tests that all peers are discovered on startup
func TestPeerDiscoveryOnStartup(t *testing.T) {

}

/************** shutdown tests **************/

/************** query tests **************/



