/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/4/13
 * Time: 10:06 AM
 * To change this template use File | Settings | File Templates.
 */
package cluster

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"
	"code.google.com/p/go-uuid/uuid"
)


func equalityCheck(t *testing.T, name string, v1 interface {}, v2 interface{}) {
	if v1 != v2 {
		t.Errorf("%v mismatch. Expecting %v, got %v", name, v1, v2)
	} else {
		t.Logf("%v OK: %v", name, v1)
	}
}

func sliceEqualityCheck(t *testing.T, name string, v1 []byte, v2 []byte) {
	if !bytes.Equal(v1, v2) {
		t.Errorf("%v mismatch. Expecting %v, got %v", name, v1, v2)
	} else {
		t.Logf("%v OK: %v", name, v1)
	}
}


func TestConnectionRequest(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &ConnectionRequest{PeerData{
		NodeId:NodeId(uuid.NewRandom()),
		Addr:"127.0.0.1:9999",
		Name:"Test Node",
		Token:Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
	}}

	writer := bufio.NewWriter(buf)
	err := src.Serialize(writer)
	if err != nil {
		t.Fatalf("unexpected Serialize error: %v", err)
	}
	writer.Flush()

	dst := &ConnectionRequest{}
	err = dst.Deserialize(bufio.NewReader(buf))
	if err != nil {
		t.Fatalf("unexpected Deserialize error: %v", err)
	}

	// check values
	sliceEqualityCheck(t, "NodeId", src.NodeId, dst.NodeId)
	equalityCheck(t, "Addr", src.Addr, dst.Addr)
	equalityCheck(t, "Name", src.Name, dst.Name)
	sliceEqualityCheck(t, "Token", src.Token, dst.Token)

}


func TestConnectionAcceptedResponse(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &ConnectionAcceptedResponse{
		NodeId:NodeId(uuid.NewRandom()),
		Name:"Test Node",
		Token:Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
	}

	writer := bufio.NewWriter(buf)
	err := src.Serialize(writer)
	if err != nil {
		t.Fatalf("unexpected Serialize error: %v", err)
	}
	writer.Flush()

	dst := &ConnectionAcceptedResponse{}
	err = dst.Deserialize(bufio.NewReader(buf))
	if err != nil {
		t.Fatalf("unexpected Deserialize error: %v", err)
	}

	// check value
	sliceEqualityCheck(t, "NodeId", src.NodeId, dst.NodeId)
	equalityCheck(t, "Name", src.Name, dst.Name)
	sliceEqualityCheck(t, "Token", src.Token, dst.Token)
}


func TestDiscoverPeersRequest(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &DiscoverPeersRequest{
		NodeId:NodeId(uuid.NewRandom()),
	}

	writer := bufio.NewWriter(buf)
	err := src.Serialize(writer)
	if err != nil {
		t.Fatalf("unexpected Serialize error: %v", err)
	}
	writer.Flush()

	dst := &DiscoverPeersRequest{}
	err = dst.Deserialize(bufio.NewReader(buf))
	if err != nil {
		t.Fatalf("unexpected Deserialize error: %v", err)
	}

	sliceEqualityCheck(t, "NodeId", src.NodeId, dst.NodeId)
}


func TestDiscoverPeersResponse(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &DiscoverPeerResponse{
		Peers: []*PeerData{
			&PeerData{
				NodeId:NodeId(uuid.NewRandom()),
				Addr:"127.0.0.1:9998",
				Name:"Test Node1",
				Token:Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
			},
			&PeerData{
				NodeId:NodeId(uuid.NewRandom()),
				Addr:"127.0.0.1:9999",
				Name:"Test Node2",
				Token:Token([]byte{1,2,3,4,5,6,7,0,1,2,3,4,5,6,7,0}),
			},
		},
	}
	writer := bufio.NewWriter(buf)
	err := src.Serialize(writer)
	if err != nil {
		t.Fatalf("unexpected Serialize error: %v", err)
	}
	writer.Flush()


	dst := &DiscoverPeerResponse{}
	err = dst.Deserialize(bufio.NewReader(buf))
	if err != nil {
		t.Fatalf("unexpected Deserialize error: %v", err)
	}
	if len(dst.Peers) != 2 {
		t.Fatalf("expected Peers length of 2, got %v", len(dst.Peers))
	}

	for i:=0; i<2; i++ {
		s := src.Peers[i]
		d := dst.Peers[i]

		sliceEqualityCheck(t, fmt.Sprintf("NodeId:%v", i), s.NodeId, d.NodeId)
		equalityCheck(t, fmt.Sprintf("Addr:%v", i), s.Addr, d.Addr)
		equalityCheck(t, fmt.Sprintf("Name:%v", i), s.Name, d.Name)
		sliceEqualityCheck(t, fmt.Sprintf("Token:%v", i), s.Token, d.Token)
	}

}

