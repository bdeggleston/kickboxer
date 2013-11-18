package cluster

import (
	"fmt"
	"time"
	"testing"
	"testing_helpers"
)

import (
	"kvstore"
)

/************** test stream from node **************/

// tests streamFromNode method
func TestStreamFromNodeMethod(t *testing.T) {
	// TODO: test StreamRequest is sent

	// TODO: test status is set to CLUSTER_STREAMING

}

// tests that method panics if the given node is not a remote node
func TestStreamFromNodeMethodWithNonRemoteNode(t *testing.T) {

}

/************** test stream to node **************/

// tests that the appropriate keys are streamed to
// the requesting node
func TestStreamToNode(t *testing.T) {
	cluster := makeLiteralRing(10, 3)

	interval := 50
	numKeys := 10000 / interval
	store := cluster.localNode.store.(*kvstore.KVStore)
	expected := make(map[string] bool)
	for i:=0; i<numKeys; i++ {
		keyNum := i * interval
		key := fmt.Sprintf("%04d", keyNum)
		if val, err := store.ExecuteWrite("SET", key, []string{key}, time.Now()); err != nil {
			t.Fatalf("Error while writing to store: %v", err)
			_ = val
		}

		if keyNum > 2000 && keyNum <= 5000 {
			expected[key] = false
		}
	}
	testing_helpers.AssertEqual(t, "store size", numKeys, len(store.GetKeys()))

	sock := newPgmConn()
	sock.outputFactory = func(p *pgmConn) Message {
		if _, ok := p.incoming[len(p.incoming) - 1].(*StreamCompleteRequest); ok {
			return &StreamCompleteResponse{}
		}
		return &StreamDataResponse{}
	}
	targetToken := literalPartitioner{}.GetToken("5000")
	requestToken := literalPartitioner{}.GetToken("5000")
	node := cluster.ring.GetNodesForToken(requestToken, 1)[0].(*RemoteNode)
	testing_helpers.AssertSliceEqual(t, "target node token", targetToken, node.GetToken())

	testing_helpers.AssertEqual(t, "pool size", uint(0), node.pool.size)
	node.pool.Put(&Connection{socket:sock, completedHandshake:true, isClosed:false})

	if err := cluster.streamToNode(node); err != nil {
		t.Errorf("Unexpected error while streaming: %v", err)
	}

	// 61 messages should have been sent, 1 for the 60
	// keys, and the final one indicating streaming was complete
	if len(sock.incoming) != len(expected) + 1 {
		t.Errorf("Unexpected num messages received. 61 expected, got: %v", len(sock.incoming))
	}

	for i, m := range sock.incoming {
		if i < len(expected) {
			msg := m.(*StreamDataRequest)
			testing_helpers.AssertEqual(t, "data size", 1, len(msg.Data))
			key := msg.Data[0].Key
			seen, ok := expected[key]
			if seen {
				t.Errorf("Value already seen: %v", key)
			} else if !ok {
				t.Errorf("Unexpected value found: %v", key)
			} else {
				expected[key] = true
			}

		} else {
			if _, ok := m.(*StreamCompleteRequest); !ok {
				t.Errorf("Unexpected ending message. Expected StreamCompleteRequest, got %T", m)
			}
		}
	}

	// check that all the expected keys were seen
	for key, val := range expected {
		if !val {
			t.Errorf("Key [%v] not streamed to node", key)
		}
	}
}


/************** query behavior tests **************/

// writes should be routed to the streaming node and mirrored to the local node
func TestStreamingWriteRouting(t *testing.T) {

}

// reads should be routed directly to the streaming nodes
func TestStreamingReadRouting(t *testing.T) {

}

/************** streaming data tests **************/

