package cluster

import (
	"bufio"
	"bytes"
	"testing"
)

import (
	"testing_helpers"
)

func TestStreamRequest(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &StreamRequest{}

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
	if mtype != STREAM_REQUEST {
		t.Fatalf("unexpected message type enum: %v", mtype)
	}
	_, ok := msg.(*StreamRequest)
	if !ok {
		t.Fatalf("unexpected message type %T", msg)
	}
}

func TestStreamResponse(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &StreamResponse{}

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
	if mtype != STREAM_RESPONSE {
		t.Fatalf("unexpected message type enum: %v", mtype)
	}
	_, ok := msg.(*StreamResponse)
	if !ok {
		t.Fatalf("unexpected message type %T", msg)
	}
}

func TestStreamCompleteRequest(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &StreamCompleteRequest{}

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
	if mtype != STREAM_COMPLETE_REQUEST {
		t.Fatalf("unexpected message type enum: %v", mtype)
	}
	_, ok := msg.(*StreamCompleteRequest)
	if !ok {
		t.Fatalf("unexpected message type %T", msg)
	}
}

func TestStreamCompleteResponse(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &StreamCompleteResponse{}

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
	if mtype != STREAM_COMPLETE_RESPONSE {
		t.Fatalf("unexpected message type enum: %v", mtype)
	}
	_, ok := msg.(*StreamCompleteResponse)
	if !ok {
		t.Fatalf("unexpected message type %T", msg)
	}
}

func TestStreamDataStruct(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &StreamData{Key: "blake", Data: []byte("eggleston")}
	src.Serialize(bufio.NewWriter(buf))
	dst := &StreamData{}
	dst.Deserialize(bufio.NewReader(buf))

	testing_helpers.AssertEqual(t, "Key", src.Key, dst.Key)
	testing_helpers.AssertSliceEqual(t, "Data", src.Data, dst.Data)
}

func TestStreamDataRequest(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &StreamDataRequest{Data: []*StreamData{
		&StreamData{Key: "blake", Data: []byte("eggleston")},
		&StreamData{Key: "travis", Data: []byte("eggleston")},
		&StreamData{Key: "cameron", Data: []byte("eggleston")},
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
	if mtype != STREAM_DATA_REQUEST {
		t.Fatalf("unexpected message type enum: %v", mtype)
	}
	dst, ok := msg.(*StreamDataRequest)
	if !ok {
		t.Fatalf("unexpected message type %T", msg)
	}
	testing_helpers.AssertEqual(t, "data len", len(src.Data), len(dst.Data))
	for i := 0; i < 3; i++ {
		testing_helpers.AssertEqual(t, "Key", src.Data[i].Key, dst.Data[i].Key)
		testing_helpers.AssertSliceEqual(t, "Data", src.Data[i].Data, dst.Data[i].Data)
	}

}

func TestStreamDataResponse(t *testing.T) {
	buf := &bytes.Buffer{}
	src := &StreamDataResponse{}

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
	if mtype != STREAM_DATA_RESPONSE {
		t.Fatalf("unexpected message type enum: %v", mtype)
	}
	_, ok := msg.(*StreamDataResponse)
	if !ok {
		t.Fatalf("unexpected message type %T", msg)
	}

}
