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
