/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/8/13
 * Time: 9:39 PM
 * To change this template use File | Settings | File Templates.
 */
package cluster

import (
	"testing"
)


func setupCluster() *Cluster {
	c, _ := NewCluster(
		"127.0.0.1:9999",
		"Test Cluster",
		Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
		NewNodeId(),
	)
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
