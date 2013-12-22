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
	Command *Command
	Dependencies []*Command
}

func (m *PreAcceptRequest) Serialize(buf *bufio.Writer) error {
	return nil
}

func (m *PreAcceptRequest) Deserialize(buf *bufio.Writer) error {
	return nil
}

func (m *PreAcceptRequest) GetType() uint32 { return CONSENSUS_PRE_ACCEPT_REQUEST }

type PreAcceptResponse struct {
	Accepted bool

	// the replica's view of dependencies
	// will be returned if the request
	// is not accepted
	Dependencies []*Command
}

func (m *PreAcceptResponse) Serialize(buf *bufio.Writer) error {
	return nil
}

func (m *PreAcceptResponse) Deserialize(buf *bufio.Writer) error {
	return nil
}

func (m *PreAcceptResponse) GetType() uint32 { return CONSENSUS_PRE_ACCEPT_RESPONSE }

type CommitRequest struct {

}

func (m *CommitRequest) Serialize(buf *bufio.Writer) error {
	return nil
}

func (m *CommitRequest) Deserialize(buf *bufio.Writer) error {
	return nil
}

func (m *CommitRequest) GetType() uint32 { return CONSENSUS_COMMIT_REQUEST }

type CommitResponse struct {

}

func (m *CommitResponse) Serialize(buf *bufio.Writer) error {
	return nil
}

func (m *CommitResponse) Deserialize(buf *bufio.Writer) error {
	return nil
}

func (m *CommitResponse) GetType() uint32 { return CONSENSUS_COMMIT_RESPONSE }

type AcceptRequest struct {

}

func (m *AcceptRequest) Serialize(buf *bufio.Writer) error {
	return nil
}

func (m *AcceptRequest) Deserialize(buf *bufio.Writer) error {
	return nil
}

func (m *AcceptRequest) GetType() uint32 { return CONSENSUS_ACCEPT_REQUEST }

type AcceptResponse struct {

}

func (m *AcceptResponse) Serialize(buf *bufio.Writer) error {
	return nil
}

func (m *AcceptResponse) Deserialize(buf *bufio.Writer) error {
	return nil
}

func (m *AcceptResponse) GetType() uint32 { return CONSENSUS_ACCEPT_RESPONSE }

// ----------- encoding helpers -----------

func serializeCommand(cmd *Command, buf *bufio.Writer) error {
	if err := writeFieldString(buf, string(cmd.LeaderID)); err != nil { return err }
	if err := binary.Write(buf, binary.LittleEndian, &cmd.Status); err != nil { return err }
	if err := writeFieldString(buf, cmd.Cmd); err != nil { return err }
	if err := writeFieldString(buf, cmd.Key); err != nil { return err }

	// Args
	numArgs := uint32(len(cmd.Args))
	if err := binary.Write(buf, binary.LittleEndian, &numArgs); err != nil { return err }
	for _, arg := range cmd.Args {
		if err := writeFieldString(buf, arg); err != nil { return err }
	}

	if err := writeTime(buf, cmd.Timestamp); err != nil { return err }
	var blocking byte
	if cmd.Blocking { blocking = 0x1 }
	if err := binary.Write(buf, binary.LittleEndian, &blocking); err != nil { return err }
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

	if err = binary.Read(buf, binary.LittleEndian, &cmd.Status); err != nil { return nil, err }
	if cmd.Cmd, err = readFieldString(buf); err != nil { return nil, err }
	if cmd.Key, err = readFieldString(buf); err != nil { return nil, err }

	// Args
	var numArgs uint32
	if err := binary.Read(buf, binary.LittleEndian, &numArgs); err != nil { return nil, err }
	cmd.Args = make([]string, numArgs)
	for i:=uint32(0); i<numArgs; i++ {
		if cmd.Args[i], err = readFieldString(buf); err != nil { return nil, err }
	}

	if cmd.Timestamp, err = readTime(buf); err != nil { return nil, err }

	var blocking byte
	if err := binary.Read(buf, binary.LittleEndian, &blocking); err != nil { return nil, err }
	cmd.Blocking = blocking != 0x00
	if err := binary.Read(buf, binary.LittleEndian, &cmd.Ballot); err != nil { return nil, err }

	return cmd, nil
}

