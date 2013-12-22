package cluster

import (
	"bufio"
	"bytes"
	"testing"
	"time"

)
import (
	"testing_helpers"
)

func TestPreAcceptRequest(t *testing.T) {
	src := &PreAcceptRequest{
		Command:&Command{
			LeaderID: NewNodeId(),
			Status: DS_EXECUTED,
			Cmd: "SET",
			Key: "ABCXYZ",
			Args: []string{"d", "e", "f"},
			Timestamp: time.Now(),
			Blocking: true,
			Ballot: uint64(2002),
		},
		Dependencies: []*Command{
			&Command{
				LeaderID: NewNodeId(),
				Status: DS_ACCEPTED,
				Cmd: "GET",
				Key: "DEF",
				Args: []string{"g", "h", "i"},
				Timestamp: time.Now(),
				Blocking: false,
				Ballot: uint64(2001),
			},
			&Command{
				LeaderID: NewNodeId(),
				Status: DS_EXECUTED,
				Cmd: "DEL",
				Key: "ABCXYZ",
				Args: []string{"d", "e", "f"},
				Timestamp: time.Now(),
				Blocking: true,
				Ballot: uint64(2000),
			},
		},
	}
	// interface check
	_ = Message(src)

	buf := &bytes.Buffer{}
	// write, then read message
	if err := WriteMessage(buf, src); err != nil {
		t.Fatalf("unexpected Serialize error: %v", err)
	}
	msg, mtype, err := ReadMessage(buf)
	if err != nil {
		t.Fatalf("unexpected Deserialize error: %v", err)
	}
	if mtype != CONSENSUS_PRE_ACCEPT_REQUEST {
		t.Fatalf("unexpected message type enum: %v", mtype)
	}
	dst, ok := msg.(*PreAcceptRequest)
	if !ok {
		t.Fatalf("unexpected message type %T", msg)
	}

	if !src.Command.Equal(dst.Command) {
		t.Errorf("src Command doesn't match dst command. Expected: %+v, got %+v", src, dst)
	}
	if !testing_helpers.AssertEqual(t, "Arg sizes", len(src.Dependencies), len(dst.Dependencies)) {
		t.FailNow()
	}
	for i:=0;i<len(src.Dependencies);i++ {
		s, d := src.Dependencies[i], dst.Dependencies[i]
		if !s.Equal(d) {
			t.Errorf("src Dependecy %v doesn't match dst command. Expected: %+v, got %+v", i, s, d)
		}
	}
}

func TestPreAcceptResponse(t *testing.T) {
	src := &PreAcceptResponse{
		Accepted:true,
		Dependencies: []*Command{
			&Command{
				LeaderID: NewNodeId(),
				Status: DS_ACCEPTED,
				Cmd: "GET",
				Key: "DEF",
				Args: []string{"g", "h", "i"},
				Timestamp: time.Now(),
				Blocking: false,
				Ballot: uint64(2001),
			},
			&Command{
				LeaderID: NewNodeId(),
				Status: DS_EXECUTED,
				Cmd: "DEL",
				Key: "ABCXYZ",
				Args: []string{"d", "e", "f"},
				Timestamp: time.Now(),
				Blocking: true,
				Ballot: uint64(2000),
			},
		},
	}
	// interface check
	_ = Message(src)

	buf := &bytes.Buffer{}
	// write, then read message
	if err := WriteMessage(buf, src); err != nil {
		t.Fatalf("unexpected Serialize error: %v", err)
	}
	msg, mtype, err := ReadMessage(buf)
	if err != nil {
		t.Fatalf("unexpected Deserialize error: %v", err)
	}
	if mtype != CONSENSUS_PRE_ACCEPT_RESPONSE {
		t.Fatalf("unexpected message type enum: %v", mtype)
	}
	dst, ok := msg.(*PreAcceptResponse)
	if !ok {
		t.Fatalf("unexpected message type %T", msg)
	}

	testing_helpers.AssertEqual(t, "Accepted", src.Accepted, dst.Accepted)
	if !testing_helpers.AssertEqual(t, "Arg sizes", len(src.Dependencies), len(dst.Dependencies)) {
		t.FailNow()
	}
	for i:=0;i<len(src.Dependencies);i++ {
		s, d := src.Dependencies[i], dst.Dependencies[i]
		if !s.Equal(d) {
			t.Errorf("src Dependecy %v doesn't match dst command. Expected: %+v, got %+v", i, s, d)
		}
	}

}

func TestCommitRequest(t *testing.T) {

}

func TestCommitResponse(t *testing.T) {

}

func TestAcceptRequest(t *testing.T) {

}

func TestAcceptResponse(t *testing.T) {

}

func TestCommandSerialization(t *testing.T) {
	src := &Command{
		LeaderID: NewNodeId(),
		Status: DS_EXECUTED,
		Cmd: "SET",
		Key: "ABCXYZ",
		Args: []string{"d", "e", "f"},
		Timestamp: time.Now(),
		Blocking: true,
		Ballot: uint64(2002),
	}

	buf := &bytes.Buffer{}
	writer := bufio.NewWriter(buf)
	if err := serializeCommand(src, writer); err != nil {
		t.Fatalf("Error serializing command: %v", err)
	}
	writer.Flush()

	dst, err := deserializeCommand(bufio.NewReader(buf))
	if err != nil {
		t.Fatalf("Error deserializing command: %v", err)
	}


	if !src.Equal(dst) {
		t.Errorf("src & dst are not equal, expected: %+v, got", src, dst)
	}
	testing_helpers.AssertEqual(t, "LeaderID", src.LeaderID, dst.LeaderID)
	testing_helpers.AssertEqual(t, "Status", src.Status, dst.Status)
	testing_helpers.AssertEqual(t, "Cmd", src.Cmd, dst.Cmd)
	testing_helpers.AssertEqual(t, "Key", src.Key, dst.Key)
	testing_helpers.AssertStringArrayEqual(t, "Args", src.Args, dst.Args)
	testing_helpers.AssertEqual(t, "Blocking", src.Blocking, dst.Blocking)
	testing_helpers.AssertEqual(t, "Ballot", src.Ballot, dst.Ballot)
	if !src.Timestamp.Equal(dst.Timestamp) {
		t.Errorf("Timestamp mismatch. Expected: %v, got %v", src.Timestamp, dst.Timestamp)
	}
}
