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
	if err := writeFieldBytes(buf, []byte(cmd.LeaderID)); err != nil { return err }
	if err := binary.Write(buf, binary.LittleEndian, &cmd.Status); err != nil { return err }
	if err := writeFieldBytes(buf, []byte(cmd.Cmd)); err != nil { return err }
	if err := writeFieldBytes(buf, []byte(cmd.Key)); err != nil { return err }

	// Args
	numArgs := len(cmd.Args)
	if err := binary.Write(buf, binary.LittleEndian, &numArgs); err != nil { return err }
	for _, arg := range cmd.Args {
		if err := writeFieldBytes(buf, []byte(arg)); err != nil { return err }
	}


	if b, err := cmd.Timestamp.GobEncode(); err != nil {
		return err
	} else {
		if err := writeFieldBytes(buf, b); err != nil { return err }
	}
	return nil
}

