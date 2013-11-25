package cluster

import (
	"fmt"
	"testing"
	"testing_helpers"
)

// tests add node behavior
func TestAddNode(t *testing.T) {
	dc := setupDC(3, 10)

	testing_helpers.AssertEqual(t, "num dcs", 3, len(dc.rings))
	for i, dcid := range []DatacenterId{"DC1", "DC2", "DC3"} {
		dcNum := i + 1
		ring, exists := dc.rings[dcid]
		if !exists {
			t.Fatalf("DC not found: [%v]", dcid)
		}
		nodes := ring.AllNodes()
		testing_helpers.AssertEqual(t, "ring size", 10, len(nodes))
		node := nodes[0]

		testing_helpers.AssertEqual(t, "dc name", DatacenterId(fmt.Sprintf("DC%v", dcNum)), node.GetDatacenterId())
	}
}

func TestGetRing(t *testing.T) {
	dc := setupDC(3, 10)

	ring, err := dc.GetRing("DC1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ring == nil {
		t.Fatalf("Ring unexpectedly nil")
	}

	ring, err = dc.GetRing("DC5")
	if err == nil {
		t.Fatalf("Expected error, got nil")
	}
	if ring != nil {
		t.Fatalf("Expected nil ring")
	}
}

func TestGetNodesForToken(t *testing.T) {
	dc := setupDC(3, 10)

	var token Token

	token = Token([]byte{0,0,4,5})
	nodes := dc.GetNodesForToken(token, 3)

	if len(nodes) != 9 { t.Fatalf("wrong number of nodes returned, expected 3, got %v", len(nodes)) }
	for i:=0; i<3; i++ {
		baseIdx := i * 3
		dcid := nodes[baseIdx].GetDatacenterId()
		testing_helpers.AssertEqual(t, "node[0]", dc.rings[dcid].tokenRing[5].GetId(), nodes[baseIdx + 0].GetId())
		testing_helpers.AssertEqual(t, "node[1]", dc.rings[dcid].tokenRing[6].GetId(), nodes[baseIdx + 1].GetId())
		testing_helpers.AssertEqual(t, "node[2]", dc.rings[dcid].tokenRing[7].GetId(), nodes[baseIdx + 2].GetId())
	}
}


