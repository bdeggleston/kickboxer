package cluster

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"testing"
	"time"
)

import (
	"message"
	"testing_helpers"
)

func TestReadRequest(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &ReadRequest{
		Cmd:  "GET",
		Key:  "A",
		Args: []string{"B", "C"},
	}

	// write, then read message
	if err := message.WriteMessage(buf, src); err != nil {
		t.Fatalf("unexpected Serialize error: %v", err)
	}
	msg, err := message.ReadMessage(buf)
	if err != nil {
		t.Fatalf("unexpected Deserialize error: %v", err)
	}
	dst, ok := msg.(*ReadRequest)
	if !ok {
		t.Fatalf("unexpected message type %T", msg)
	}

	testing_helpers.AssertEqual(t, "Type", READ_REQUEST, dst.GetType())
	testing_helpers.AssertEqual(t, "Cmd", src.Cmd, dst.Cmd)
	testing_helpers.AssertEqual(t, "Key", src.Key, dst.Key)
	testing_helpers.AssertEqual(t, "Arg len", len(src.Args), len(dst.Args))
	testing_helpers.AssertEqual(t, "Arg[0]", src.Args[0], dst.Args[0])
	testing_helpers.AssertEqual(t, "Arg[1]", src.Args[1], dst.Args[1])
}

func TestWriteRequest(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &WriteRequest{
		ReadRequest: ReadRequest{
			Cmd:  "GET",
			Key:  "A",
			Args: []string{"B", "C"},
		},
		Timestamp: time.Now(),
	}

	// write, then read message
	if err := message.WriteMessage(buf, src); err != nil {
		t.Fatalf("unexpected Serialize error: %v", err)
	}
	msg, err := message.ReadMessage(buf)
	if err != nil {
		t.Fatalf("unexpected Deserialize error: %v", err)
	}
	dst, ok := msg.(*WriteRequest)
	if !ok {
		t.Fatalf("unexpected message type %T", msg)
	}

	testing_helpers.AssertEqual(t, "Type", WRITE_REQUEST, dst.GetType())
	testing_helpers.AssertEqual(t, "Cmd", src.Cmd, dst.Cmd)
	testing_helpers.AssertEqual(t, "Key", src.Key, dst.Key)
	testing_helpers.AssertEqual(t, "Arg len", len(src.Args), len(dst.Args))
	testing_helpers.AssertEqual(t, "Arg[0]", src.Args[0], dst.Args[0])
	testing_helpers.AssertEqual(t, "Arg[1]", src.Args[1], dst.Args[1])
	testing_helpers.AssertEqual(t, "Timestamp", src.Timestamp, dst.Timestamp)
}

func TestQueryResponse(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &QueryResponse{
		Data: [][]byte{
			[]byte(uuid.NewRandom()),
			[]byte(uuid.NewRandom()),
			[]byte(uuid.NewRandom()),
		},
	}

	// write, then read message
	if err := message.WriteMessage(buf, src); err != nil {
		t.Fatalf("unexpected Serialize error: %v", err)
	}
	msg, err := message.ReadMessage(buf)
	if err != nil {
		t.Fatalf("unexpected Deserialize error: %v", err)
	}
	dst, ok := msg.(*QueryResponse)
	if !ok {
		t.Fatalf("unexpected message type %T", msg)
	}

	testing_helpers.AssertEqual(t, "Type", QUERY_RESPONSE, dst.GetType())
	testing_helpers.AssertEqual(t, "Data len", len(src.Data), len(dst.Data))
	testing_helpers.AssertSliceEqual(t, "Data[0]", src.Data[0], dst.Data[0])
	testing_helpers.AssertSliceEqual(t, "Data[1]", src.Data[1], dst.Data[1])
	testing_helpers.AssertSliceEqual(t, "Data[2]", src.Data[2], dst.Data[2])
}
