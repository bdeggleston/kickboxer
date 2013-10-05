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
	"testing"
	"code.google.com/p/go-uuid/uuid"
)


func TestConnectionRequest(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &ConnectionRequest{
		NodeId:NodeId(uuid.NewRandom()),
		Addr:"127.0.0.1:9999",
		Name:"Test Node",
		Token:Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
	}

	writer := bufio.NewWriter(buf)
	err := src.Serialize(writer)
	if err != nil {
		t.Fatalf("unexpected Serialize err: %v", err)
	}
	writer.Flush()

	dst := &ConnectionRequest{}
	err = dst.Deserialize(bufio.NewReader(buf))
	if err != nil {
		t.Fatalf("unexpected Deserialize err: %v", err)
	}

	// check values
	if !bytes.Equal(src.NodeId, dst.NodeId) {
		t.Errorf("NodeId mismatch. Expecting %v, got %v", src.NodeId, dst.NodeId)
	} else {
		t.Logf("NodeId OK: %v", dst.NodeId)
	}

	if src.Addr != dst.Addr {
		t.Errorf("Addr mismatch. Expecting %v, got %v", src.Addr, dst.Addr)
	} else {
		t.Logf("Addr OK: %v", dst.Addr)
	}

	if src.Name != dst.Name {
		t.Errorf("Name mismatch. Expecting %v, got %v", src.Name, dst.Name)
	} else {
		t.Logf("Name OK: %v", dst.Name)
	}

	if !bytes.Equal(src.Token, dst.Token) {
		t.Errorf("Token mismatch. Expecting %v, got %v", src.Token, dst.Token)
	} else {
		t.Logf("Token OK: %v", dst.Token)
	}

}


func TestConnectionAcceptedResponse(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &ConnectionRequest{
		NodeId:NodeId(uuid.NewRandom()),
		Name:"Test Node",
		Token:Token([]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
	}

	writer := bufio.NewWriter(buf)
	err := src.Serialize(writer)
	if err != nil {
		t.Fatalf("unexpected Serialize err: %v", err)
	}
	writer.Flush()

	dst := &ConnectionRequest{}
	err = dst.Deserialize(bufio.NewReader(buf))
	if err != nil {
		t.Fatalf("unexpected Deserialize err: %v", err)
	}

	// check values
	if !bytes.Equal(src.NodeId, dst.NodeId) {
		t.Errorf("NodeId mismatch. Expecting %v, got %v", src.NodeId, dst.NodeId)
	} else {
		t.Logf("NodeId OK: %v", dst.NodeId)
	}

	if src.Name != dst.Name {
		t.Errorf("Name mismatch. Expecting %v, got %v", src.Name, dst.Name)
	} else {
		t.Logf("Name OK: %v", dst.Name)
	}

	if !bytes.Equal(src.Token, dst.Token) {
		t.Errorf("Token mismatch. Expecting %v, got %v", src.Token, dst.Token)
	} else {
		t.Logf("Token OK: %v", dst.Token)
	}


}

