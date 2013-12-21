package cluster

import (
	"bytes"
	"fmt"
	"testing"
)

import (
	"testing_helpers"
)

func TestConnectionRequest(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &ConnectionRequest{PeerData{
		NodeId:NewNodeId(),
		DCId:"DC5000",
		Addr:"127.0.0.1:9999",
		Name:"Test Node",
		Token:Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
	}}

	// interface check
	_ = Message(src)

	// write, then read message
	if err := WriteMessage(buf, src); err != nil {
		t.Fatalf("unexpected Serialize error: %v", err)
	}
	msg, mtype, err := ReadMessage(buf)
	if err != nil {
		t.Fatalf("unexpected Deserialize error: %v", err)
	}
	if mtype != CONNECTION_REQUEST {
		t.Fatalf("unexpected message type enum: %v", mtype)
	}
	dst, ok := msg.(*ConnectionRequest)
	if !ok {
		t.Fatalf("unexpected message type %T", msg)
	}

	// check values
	testing_helpers.AssertEqual(t, "Type", CONNECTION_REQUEST, dst.GetType())
	testing_helpers.AssertEqual(t, "NodeId", src.NodeId, dst.NodeId)
	testing_helpers.AssertEqual(t, "DCId", src.DCId, dst.DCId)
	testing_helpers.AssertEqual(t, "Addr", src.Addr, dst.Addr)
	testing_helpers.AssertEqual(t, "Name", src.Name, dst.Name)
	testing_helpers.AssertSliceEqual(t, "Token", src.Token, dst.Token)

}


func TestConnectionAcceptedResponse(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &ConnectionAcceptedResponse{
		NodeId:NewNodeId(),
		DCId:"DC5000",
		Name:"Test Node",
		Token:Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
	}

	// interface check
	_ = Message(src)

	// write, then read message
	if err := WriteMessage(buf, src); err != nil {
		t.Fatalf("unexpected Serialize error: %v", err)
	}
	msg, mtype, err := ReadMessage(buf)
	if err != nil {
		t.Fatalf("unexpected Deserialize error: %v", err)
	}
	if mtype != CONNECTION_ACCEPTED_RESPONSE {
		t.Fatalf("unexpected message type enum: %v", mtype)
	}
	dst, ok := msg.(*ConnectionAcceptedResponse)
	if !ok {
		t.Fatalf("unexpected message type %T", msg)
	}

	// check value
	testing_helpers.AssertEqual(t, "Type", CONNECTION_ACCEPTED_RESPONSE, dst.GetType())
	testing_helpers.AssertEqual(t, "NodeId", src.NodeId, dst.NodeId)
	testing_helpers.AssertEqual(t, "DCId", src.DCId, dst.DCId)
	testing_helpers.AssertEqual(t, "Name", src.Name, dst.Name)
	testing_helpers.AssertSliceEqual(t, "Token", src.Token, dst.Token)
}

func TestConnectionRefusedResponse(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &ConnectionRefusedResponse{Reason:"you suck"}

	// interface check
	_ = Message(src)

	// write, then read message
	if err := WriteMessage(buf, src); err != nil {
		t.Fatalf("unexpected Serialize error: %v", err)
	}
	msg, mtype, err := ReadMessage(buf)
	if err != nil {
		t.Fatalf("unexpected Deserialize error: %v", err)
	}
	if mtype != CONNECTION_REFUSED_RESPONSE {
		t.Fatalf("unexpected message type enum: %v", mtype)
	}
	dst, ok := msg.(*ConnectionRefusedResponse)
	if !ok {
		t.Fatalf("unexpected message type %T", msg)
	}

	// check value
	testing_helpers.AssertEqual(t, "Type", CONNECTION_REFUSED_RESPONSE, dst.GetType())
	testing_helpers.AssertEqual(t, "Reason", src.Reason, dst.Reason)
}

func TestDiscoverPeersRequest(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &DiscoverPeersRequest{
		NodeId:NewNodeId(),
	}

	// interface check
	_ = Message(src)

	// write, then read message
	if err := WriteMessage(buf, src); err != nil {
		t.Fatalf("unexpected Serialize error: %v", err)
	}
	msg, mtype, err := ReadMessage(buf)
	if err != nil {
		t.Fatalf("unexpected Deserialize error: %v", err)
	}
	if mtype != DISCOVER_PEERS_REQUEST {
		t.Fatalf("unexpected message type enum: %v", mtype)
	}
	dst, ok := msg.(*DiscoverPeersRequest)
	if !ok {
		t.Fatalf("unexpected message type %T", msg)
	}

	testing_helpers.AssertEqual(t, "Type", DISCOVER_PEERS_REQUEST, dst.GetType())
	testing_helpers.AssertEqual(t, "NodeId", src.NodeId, dst.NodeId)
}


func TestDiscoverPeersResponse(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &DiscoverPeerResponse{
		Peers: []*PeerData{
			&PeerData{
				NodeId:NewNodeId(),
				DCId:DatacenterId("DC5000"),
				Addr:"127.0.0.1:9998",
				Name:"Test Node1",
				Token:Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
			},
			&PeerData{
				NodeId:NewNodeId(),
				DCId:DatacenterId("DC2000"),
				Addr:"127.0.0.1:9999",
				Name:"Test Node2",
				Token:Token([]byte{1,2,3,4,5,6,7,0,1,2,3,4,5,6,7,0}),
			},
		},
	}

	// interface check
	_ = Message(src)

	// write, then read message
	if err := WriteMessage(buf, src); err != nil {
		t.Fatalf("unexpected Serialize error: %v", err)
	}
	msg, mtype, err := ReadMessage(buf)
	if err != nil {
		t.Fatalf("unexpected Deserialize error: %v", err)
	}
	if mtype != DISCOVER_PEERS_RESPONSE {
		t.Fatalf("unexpected message type enum: %v", mtype)
	}
	dst, ok := msg.(*DiscoverPeerResponse)
	if !ok {
		t.Fatalf("unexpected message type %T", msg)
	}

	testing_helpers.AssertEqual(t, "Type", DISCOVER_PEERS_RESPONSE, dst.GetType())
	for i:=0; i<2; i++ {
		s := src.Peers[i]
		d := dst.Peers[i]

		testing_helpers.AssertEqual(t, fmt.Sprintf("NodeId:%v", i), s.NodeId, d.NodeId)
		testing_helpers.AssertEqual(t, fmt.Sprintf("DCId:%v", i), s.DCId, d.DCId)
		testing_helpers.AssertEqual(t, fmt.Sprintf("Addr:%v", i), s.Addr, d.Addr)
		testing_helpers.AssertEqual(t, fmt.Sprintf("Name:%v", i), s.Name, d.Name)
		testing_helpers.AssertSliceEqual(t, fmt.Sprintf("Token:%v", i), s.Token, d.Token)
	}

}



