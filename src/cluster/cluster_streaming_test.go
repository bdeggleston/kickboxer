package cluster

import (
	"fmt"
	"time"
	"testing"
	"testing_helpers"
)

import (
	"kvstore"
	"message"
)

// TODO: values need to be reconciled on cluster join

/************** test stream from node **************/

// tests streamFromNode method
func TestStreamFromNodeMethod(t *testing.T) {
	cluster := makeLiteralRing(10, 3)

	// get target node
	targetToken := literalPartitioner{}.GetToken("5000")
	n := cluster.topology.GetLocalNodesForToken(targetToken)[0].(*RemoteNode)
	testing_helpers.AssertSliceEqual(t, "target node token", targetToken, n.GetToken())

	// setup mock socket
	sock := newPgmConn()
	sock.outputFactory = func(_ *pgmConn) message.Message {
		return &StreamResponse{}
	}
	n.pool.Put(&Connection{socket:sock, completedHandshake:true, isClosed:false})

	if err := cluster.streamFromNode(n); err != nil {
		t.Errorf("Unexpected error requesting stream: %v", err)
	}

	// test StreamRequest is sent
	if len(sock.incoming) != 1 {
		t.Errorf("Unexpected num messages received. 1 expected, got: %v", len(sock.incoming))
	}

	msg := sock.incoming[0]
	_, ok := msg.(*StreamRequest)
	if !ok {
		t.Fatalf("Expected StreamResponse, got %T", msg)
	}

	// test status is set to CLUSTER_STREAMING
	testing_helpers.AssertEqual(t, "cluster status", CLUSTER_STREAMING, cluster.status)
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
		if val, err := store.ExecuteQuery("SET", key, []string{key}, time.Now()); err != nil {
			t.Fatalf("Error while writing to store: %v", err)
			_ = val
		}

		if keyNum > 2000 && keyNum <= 5000 {
			expected[key] = false
		}
	}
	testing_helpers.AssertEqual(t, "store size", numKeys, len(store.GetKeys()))

	// setup mock socket for node
	sock := newPgmConn()
	sock.outputFactory = func(p *pgmConn) message.Message {
		if _, ok := p.incoming[len(p.incoming) - 1].(*StreamCompleteRequest); ok {
			return &StreamCompleteResponse{}
		}
		return &StreamDataResponse{}
	}
	targetToken := literalPartitioner{}.GetToken("5000")
	n := cluster.topology.GetLocalNodesForToken(targetToken)[0].(*RemoteNode)
	testing_helpers.AssertSliceEqual(t, "target node token", targetToken, n.GetToken())

	testing_helpers.AssertEqual(t, "pool size", uint(0), n.pool.size)
	n.pool.Put(&Connection{socket:sock, completedHandshake:true, isClosed:false})

	if err := cluster.streamToNode(n); err != nil {
		t.Errorf("Unexpected error while streaming: %v", err)
	}

	// 61 messages should have been sent, 1 for each of the 60
	// keys, and a final one indicating streaming was complete
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

// tests that streaming is resumed if streaming is
// interrupted
func TestRestartStreamToNode(t *testing.T) {
	t.Skip("not implemented yet")
}


/************** query behavior tests **************/

func TestBasicStreamReceive(t *testing.T) {

}

// test that reconciliation is performed if the
// received value doesn't match the value on this
// node
func TestMismatchedStreamReceive(t *testing.T) {

}


/************** query behavior tests **************/

// writes should be routed to the streaming node and mirrored to the local node
func TestStreamingWriteRouting(t *testing.T) {

}

// reads should be routed directly to the streaming nodes
func TestStreamingReadRouting(t *testing.T) {

}

/************** streaming server tests **************/

// tests that the peer server handles a StreamRequest message properly
func TestServerStreamRequest(t *testing.T) {
	cluster := makeLiteralRing(10, 3)

	// get target node
	targetToken := literalPartitioner{}.GetToken("5000")
	n := cluster.topology.GetLocalNodesForToken(targetToken)[0].(*RemoteNode)
	testing_helpers.AssertSliceEqual(t, "target node token", targetToken, n.GetToken())

	// setup mock socket
	sock := newPgmConn()
	sock.outputFactory = func(_ *pgmConn) message.Message { return &StreamCompleteResponse{} }
	n.pool.Put(&Connection{socket:sock, completedHandshake:true, isClosed:false})

	// process message and check response
	server := &PeerServer{cluster:cluster}
	resp, err := server.executeRequest(n, &StreamRequest{})
	if err != nil {
		t.Fatalf("Unexpected error executing StreamRequest: %v", err)
	}
	// block, allow streaming goroutine to run
	time.Sleep(1)

	var ok bool
	_, ok = resp.(*StreamResponse)
	if !ok {
		t.Errorf("Expected StreamResponse, got %T", resp)
	}

	// check that the socket got a StreamCompleteResponse (there wasn't any data to stream)
	if len(sock.incoming) != 1 {
		t.Fatalf("Unexpected num messages received. 1 expected, got: %v", len(sock.incoming))
	}

	msg := sock.incoming[0]
	_, ok = msg.(*StreamCompleteRequest)
	if !ok {
		t.Fatalf("Expected StreamCompleteRequest, got %T", msg)
	}

}

// tests that the peer server handles a StreamDataRequest message properly
func TestServerStreamDataRequest(t *testing.T) {

}

// tests that the peer server handles a StreamCompleteRequest message properly
func TestServerStreamCompleteRequest(t *testing.T) {

}
