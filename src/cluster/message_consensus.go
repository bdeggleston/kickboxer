package cluster

import (
	"bufio"
	"encoding/binary"
)

const (
	CONSENSUS_QUERY_FORWARD = uint32(501)
	CONSENSUS_PRE_ACCEPT_REQUEST = uint32(503)
	CONSENSUS_PRE_ACCEPT_RESPONSE = uint32(504)
	CONSENSUS_COMMIT_REQUEST = uint32(505)
	CONSENSUS_COMMIT_RESPONSE = uint32(506)
	CONSENSUS_ACCEPT_REQUEST = uint32(507)
	CONSENSUS_ACCEPT_RESPONSE = uint32(508)
)

// ----------- consensus messages -----------

type PreAcceptRequest struct {
	LeaderID NodeId
	Command *Command
	Dependencies []*Command
}

type PreAcceptResponse struct {
	Accepted bool

	// the replica's view of dependencies
	// will be returned if the request
	// is not accepted
	Dependencies []*Command
}

type CommitRequest struct {

}

type CommitResponse struct {

}

type AcceptRequest struct {

}

type AcceptResponse struct {

}

// ----------- encoding helpers -----------

func serializeCommand(cmd *Command, buf *bufio.Writer) error {
	if err := writeFieldString(buf, string(cmd.LeaderID)); err != nil { return err }
	if err := binary.Write(buf, binary.LittleEndian, &cmd.Status); err != nil { return err }
	if err := writeFieldString(buf, cmd.Cmd); err != nil { return err }
	if err := writeFieldString(buf, cmd.Key); err != nil { return err }

	// Args
	numArgs := len(cmd.Args)
	if err := binary.Write(buf, binary.LittleEndian, &numArgs); err != nil { return err }
	for _, arg := range cmd.Args {
		if err := writeFieldString(buf, arg); err != nil { return err }
	}

	if err := writeTime(buf, cmd.Timestamp); err != nil { return err }
	if err := binary.Write(buf, binary.LittleEndian, &cmd.Blocking); err != nil { return err }
	if err := binary.Write(buf, binary.LittleEndian, &cmd.Ballot); err != nil { return err }

	return nil
}

func deserializeCommand(buf *bufio.Reader) (*Command, error) {
	cmd := &Command{}
	var err error
	if str, err := readFieldString(buf); err != nil {
		return nil, err
	} else {
		cmd.LeaderID = NodeId(str)
	}

	if err := binary.Read(buf, binary.LittleEndian, &cmd.Status); err != nil { return nil, err }
	if cmd.Cmd, err = readFieldString(buf); err != nil { return nil, err }

	// Args
	var numArgs int
	if err := binary.Read(buf, binary.LittleEndian, &numArgs); err != nil { return nil, err }
	cmd.Args = make([]string, numArgs)
	for i:=0; i<numArgs; i++ {
		if cmd.Args[i], err = readFieldString(buf); err != nil { return nil, err }
	}

	if cmd.Timestamp, err = readTime(buf); err != nil { return nil, err }
	if err := binary.Read(buf, binary.LittleEndian, &cmd.Blocking); err != nil { return nil, err }
	if err := binary.Read(buf, binary.LittleEndian, &cmd.Ballot); err != nil { return nil, err }

	return cmd, nil
}

