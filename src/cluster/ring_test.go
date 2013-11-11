package cluster

import (
	"fmt"
	"testing"
)

import (
	"testing_helpers"
)

/************** GetNode tests **************/

func TestGetUnknownNode_(t *testing.T) {
	ring := setupRing()
	node, err := ring.getNode(NewNodeId())
	if node != nil {
		t.Errorf("Expected nil node, got: %v", node)
	}
	if err == nil {
		t.Errorf("Expected error from getNode, got nil")
	}
}

// tests that fetching a node with a valid node id
// returns the requested node
func TestGetExistingNode_(t *testing.T) {
	ring := setupRing()

	nid := ring.tokenRing[4].GetId()
	node, err := ring.getNode(nid)
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

/************** AddNode tests **************/

// tests that a node is added to the cluster if
// the cluster has not seen it yet
func TestAddingNewNodeToRing(t *testing.T) {
	ring := setupRing()

	// sanity check
	testing_helpers.AssertEqual(t, "ring map size", 10, len(ring.nodeMap))
	testing_helpers.AssertEqual(t, "ring ring size", 10, len(ring.tokenRing))

	// add a new node
	token := Token([]byte{0,0,1,2,3,4,5,6,7,0,1,2,3,4,5,6})
	newNode := newMockNode(NewNodeId(), token, "N2")
	err := ring.AddNode(newNode)
	if err != nil {
		t.Errorf("Expected nil error, got: %v", err)
	}

	testing_helpers.AssertEqual(t, "ring map size", 11, len(ring.nodeMap))
	testing_helpers.AssertEqual(t, "ring ring size", 11, len(ring.tokenRing))
	testing_helpers.AssertEqual(t, "new node started", false, newNode.IsStarted())
	testing_helpers.AssertEqual(t, "new node read size", 0, len(newNode.reads))
	testing_helpers.AssertEqual(t, "new node write size", 0, len(newNode.writes))
}

// tests that nothing is changed if a node is already
// known by the cluster, and an error is returned
func TestAddingExistingNodeToRing(t *testing.T) {
	ring := setupRing()

	// sanity check
	testing_helpers.AssertEqual(t, "ring map size", 10, len(ring.nodeMap))
	testing_helpers.AssertEqual(t, "ring size", 10, len(ring.tokenRing))

	err := ring.AddNode(ring.tokenRing[3])
	if err == nil {
		t.Errorf("Expected non nil error, got nil")
	}

	testing_helpers.AssertEqual(t, "ring map size", 10, len(ring.nodeMap))
	testing_helpers.AssertEqual(t, "ring size", 10, len(ring.tokenRing))
}

/************** AllNodes tests **************/

func TestAllNodes(t *testing.T) {
	ring := setupRing()

	nodes := ring.AllNodes()
	testing_helpers.AssertEqual(t, "node list size", len(ring.nodeMap), len(nodes))

	for i:=0; i<len(nodes); i++ {
		expected := ring.tokenRing[i]
		actual := nodes[i]

		if actual == nil {
			t.Errorf("Unexpected nil node at index [%v]", i)
		}

		testing_helpers.AssertEqual(t, fmt.Sprintf("node[%v] id", i), expected.GetId(), actual.GetId())
	}
}

/************** RefreshRing tests **************/

func TestRingIsRefreshedAfterNodeAddition_(t *testing.T) {
	ring := NewRing()

	n1 := newMockNode(
		NewNodeId(),
		Token([]byte{0,0,0,7}),
		fmt.Sprintf("N1"),
	)
	ring.AddNode(n1)

	n2 := newMockNode(
		NewNodeId(),
		Token([]byte{0,0,0,3}),
		"N2",
	)
	ring.AddNode(n2)

	testing_helpers.AssertEqual(t, "ring position 0", ring.tokenRing[0].GetId(), n2.GetId())
	testing_helpers.AssertEqual(t, "ring position 1", ring.tokenRing[1].GetId(), n1.GetId())

	n3 := newMockNode(
		NewNodeId(),
		Token([]byte{0,0,0,5}),
		"N3",
	)
	ring.AddNode(n3)

	testing_helpers.AssertEqual(t, "ring position 0", ring.tokenRing[0].GetId(), n2.GetId())
	testing_helpers.AssertEqual(t, "ring position 1", ring.tokenRing[1].GetId(), n3.GetId())
	testing_helpers.AssertEqual(t, "ring position 2", ring.tokenRing[2].GetId(), n1.GetId())

	n4 := newMockNode(
		NewNodeId(),
		Token([]byte{0,0,1,0}),
		"N4",
	)
	ring.AddNode(n4)

	testing_helpers.AssertEqual(t, "ring position 0", ring.tokenRing[0].GetId(), n2.GetId())
	testing_helpers.AssertEqual(t, "ring position 1", ring.tokenRing[1].GetId(), n3.GetId())
	testing_helpers.AssertEqual(t, "ring position 2", ring.tokenRing[2].GetId(), n1.GetId())
	testing_helpers.AssertEqual(t, "ring position 3", ring.tokenRing[3].GetId(), n4.GetId())
}

// tests that the proper nodes are returned for the given keys
func TestKeyRouting_(t *testing.T) {
	ring := setupRing()

	var token Token
	var nodes []Node

	// test the upper bound
	token = Token([]byte{0,0,9,5})
	nodes = ring.GetNodesForToken(token, 3)
	if len(nodes) != 3 { t.Fatalf("wrong number of nodes returned, expected 3, got %v", len(nodes)) }
	testing_helpers.AssertEqual(t, "node[0]", ring.tokenRing[0].GetId(), nodes[0].GetId())
	testing_helpers.AssertEqual(t, "node[1]", ring.tokenRing[1].GetId(), nodes[1].GetId())
	testing_helpers.AssertEqual(t, "node[2]", ring.tokenRing[2].GetId(), nodes[2].GetId())

	// test the lower bound
	token = Token([]byte{0,0,0,0})
	nodes = ring.GetNodesForToken(token, 3)
	if len(nodes) != 3 { t.Fatalf("wrong number of nodes returned, expected 3, got %v", len(nodes)) }
	testing_helpers.AssertEqual(t, "node[0]", ring.tokenRing[0].GetId(), nodes[0].GetId())
	testing_helpers.AssertEqual(t, "node[1]", ring.tokenRing[1].GetId(), nodes[1].GetId())
	testing_helpers.AssertEqual(t, "node[2]", ring.tokenRing[2].GetId(), nodes[2].GetId())

	// test token intersection
	token = Token([]byte{0,0,4,0})
	nodes = ring.GetNodesForToken(token, 3)
	if len(nodes) != 3 { t.Fatalf("wrong number of nodes returned, expected 3, got %v", len(nodes)) }
	testing_helpers.AssertEqual(t, "node[0]", ring.tokenRing[4].GetId(), nodes[0].GetId())
	testing_helpers.AssertEqual(t, "node[1]", ring.tokenRing[5].GetId(), nodes[1].GetId())
	testing_helpers.AssertEqual(t, "node[2]", ring.tokenRing[6].GetId(), nodes[2].GetId())

	// test middle range
	token = Token([]byte{0,0,4,5})
	nodes = ring.GetNodesForToken(token, 3)
	if len(nodes) != 3 { t.Fatalf("wrong number of nodes returned, expected 3, got %v", len(nodes)) }
	testing_helpers.AssertEqual(t, "node[0]", ring.tokenRing[5].GetId(), nodes[0].GetId())
	testing_helpers.AssertEqual(t, "node[1]", ring.tokenRing[6].GetId(), nodes[1].GetId())
	testing_helpers.AssertEqual(t, "node[2]", ring.tokenRing[7].GetId(), nodes[2].GetId())

	// test wrapping
	token = Token([]byte{0,0,8,5})
	nodes = ring.GetNodesForToken(token, 3)
	if len(nodes) != 3 { t.Fatalf("wrong number of nodes returned, expected 3, got %v", len(nodes)) }
	testing_helpers.AssertEqual(t, "node[0]", ring.tokenRing[9].GetId(), nodes[0].GetId())
	testing_helpers.AssertEqual(t, "node[1]", ring.tokenRing[0].GetId(), nodes[1].GetId())
	testing_helpers.AssertEqual(t, "node[2]", ring.tokenRing[1].GetId(), nodes[2].GetId())
}
