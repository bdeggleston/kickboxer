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
)


func TestConnectionRequest(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &ConnectionRequest{
		Addr:"127.0.0.1:9999",
		Name:"Test Node",
		Token:Token([16]byte{0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7}),
	}

	writer := bufio.NewWriter(buf)
	err := src.Serialize(writer)
	if err != nil {
		t.Fatalf("unexpected Serialize err: %v", err)
	}
	writer.Flush()

	if buf.Len() == 0 {
		t.Fatalf("byte length too low: %v", buf.Len())
	}

	dst := &ConnectionRequest{}
	err = dst.Deserialize(bufio.NewReader(buf))
	if err != nil {
		t.Fatalf("unexpected Deserialize err: %v", err)
	}

	// check values
	if src.Addr != dst.Addr {
		t.Errorf("Addr mismatch. Expecting %v, got %v", src.Addr, dst.Addr)
	}
	if src.Name != dst.Name {
		t.Errorf("Name mismatch. Expecting %v, got %v", src.Name, dst.Name)
	}
	if src.Token != dst.Token {
		t.Errorf("Token mismatch. Expecting %v, got %v", src.Token, dst.Token)
	}


}


