package cluster

import (
	"bufio"
	"encoding/binary"
)

const (
	CONSENSUS_QUERY_FORWARD = uint32(501)
	CONSENSUS_BALLOT_REJECT_RESPONSE = uint32(502)
	CONSENSUS_PRE_ACCEPT_REQUEST = uint32(503)
	CONSENSUS_PRE_ACCEPT_RESPONSE = uint32(504)
	CONSENSUS_COMMIT_REQUEST = uint32(505)
	CONSENSUS_COMMIT_RESPONSE = uint32(506)
	CONSENSUS_ACCEPT_REQUEST = uint32(507)
	CONSENSUS_ACCEPT_RESPONSE = uint32(508)
)

// ----------- consensus messages -----------

type BallotMessage interface {
	Message
	GetBallot() uint64
}

type BallotRejectResponse struct {
	Ballot uint64
}

func (m *BallotRejectResponse) Serialize(buf *bufio.Writer) error {
	if err := binary.Write(buf, binary.LittleEndian, &m.Ballot); err != nil { return err }
	return nil
}

func (m *BallotRejectResponse) Deserialize(buf *bufio.Reader) error {
	if err := binary.Read(buf, binary.LittleEndian, &m.Ballot); err != nil { return err }
	return nil
}

func (m *BallotRejectResponse) GetType() uint32 { return CONSENSUS_BALLOT_REJECT_RESPONSE }

type PreAcceptRequest struct {
	Command *Command
	Dependencies Dependencies
	Ballot uint64
}

func (m *PreAcceptRequest) Serialize(buf *bufio.Writer) error {
	if err := serializeCommand(m.Command, buf); err != nil { return err }

	numDeps := uint32(len(m.Dependencies))
	if err := binary.Write(buf, binary.LittleEndian, &numDeps); err != nil { return err }
	for _, dep := range m.Dependencies {
		if err := serializeCommand(dep, buf); err != nil { return err }
	}

	if err := binary.Write(buf, binary.LittleEndian, &m.Ballot); err != nil { return err }
	return nil
}

func (m *PreAcceptRequest) Deserialize(buf *bufio.Reader) error {
	var err error
	if m.Command, err = deserializeCommand(buf); err != nil { return err }

	var numDeps uint32
	if err = binary.Read(buf, binary.LittleEndian, &numDeps); err != nil { return err }
	m.Dependencies = make(Dependencies, int(numDeps))
	for i:=0; i<int(numDeps); i++ {
		if m.Dependencies[i], err = deserializeCommand(buf); err != nil { return err }
	}

	if err = binary.Read(buf, binary.LittleEndian, &m.Ballot); err != nil { return err }
	return nil
}

func (m *PreAcceptRequest) GetType() uint32 { return CONSENSUS_PRE_ACCEPT_REQUEST }

type PreAcceptResponse struct {
	Accepted bool

	// the replica's view of dependencies
	// will be returned if the request
	// is not accepted
	Dependencies Dependencies

	// the highest ballot number
	// the responding replica has seen
	MaxBallot uint64
}

func (m *PreAcceptResponse) Serialize(buf *bufio.Writer) error {
	var accepted byte
	if m.Accepted { accepted = 0x1 }
	if err := binary.Write(buf, binary.LittleEndian, &accepted); err != nil { return err }

	numDeps := uint32(len(m.Dependencies))
	if err := binary.Write(buf, binary.LittleEndian, &numDeps); err != nil { return err }
	for _, dep := range m.Dependencies {
		if err := serializeCommand(dep, buf); err != nil { return err }
	}

	if err := binary.Write(buf, binary.LittleEndian, &m.MaxBallot); err != nil { return err }
	return nil
}

func (m *PreAcceptResponse) Deserialize(buf *bufio.Reader) error {
	var accepted byte
	var err error
	if err = binary.Read(buf, binary.LittleEndian, &accepted); err != nil { return err }
	m.Accepted = accepted != 0x0

	var numDeps uint32
	if err = binary.Read(buf, binary.LittleEndian, &numDeps); err != nil { return err }
	m.Dependencies = make(Dependencies, int(numDeps))
	for i:=0; i<int(numDeps); i++ {
		if m.Dependencies[i], err = deserializeCommand(buf); err != nil { return err }
	}
	if err = binary.Read(buf, binary.LittleEndian, &m.MaxBallot); err != nil { return err }
	return nil
}

func (m *PreAcceptResponse) GetType() uint32 { return CONSENSUS_PRE_ACCEPT_RESPONSE }

type CommitRequest struct {
	LeaderID NodeId
	Ballot uint64
}

func (m *CommitRequest) Serialize(buf *bufio.Writer) error {
	if err := writeFieldString(buf, string(m.LeaderID)); err != nil { return err }
	if err := binary.Write(buf, binary.LittleEndian, &m.Ballot); err != nil { return err }
	return nil
}

func (m *CommitRequest) Deserialize(buf *bufio.Reader) error {
	if str, err := readFieldString(buf); err != nil {
		return err
	} else {
		m.LeaderID = NodeId(str)
	}
	if err := binary.Read(buf, binary.LittleEndian, &m.Ballot); err != nil { return err }
	return nil
}

func (m *CommitRequest) GetType() uint32 { return CONSENSUS_COMMIT_REQUEST }

type CommitResponse struct { }
func (m *CommitResponse) Serialize(buf *bufio.Writer) error { return nil }
func (m *CommitResponse) Deserialize(buf *bufio.Reader) error { return nil }
func (m *CommitResponse) GetType() uint32 { return CONSENSUS_COMMIT_RESPONSE }

type AcceptRequest struct {
	Dependencies Dependencies
}

func (m *AcceptRequest) Serialize(buf *bufio.Writer) error {
	var err error

	numDeps := uint32(len(m.Dependencies))
	if err = binary.Write(buf, binary.LittleEndian, &numDeps); err != nil { return err }
	for _, dep := range m.Dependencies {
		if err := serializeCommand(dep, buf); err != nil { return err }
	}
	return nil
}

func (m *AcceptRequest) Deserialize(buf *bufio.Reader) error {
	var err error
	var numDeps uint32
	if err = binary.Read(buf, binary.LittleEndian, &numDeps); err != nil { return err }
	m.Dependencies = make(Dependencies, int(numDeps))
	for i:=0; i<int(numDeps); i++ {
		if m.Dependencies[i], err = deserializeCommand(buf); err != nil { return err }
	}
	return nil
}

func (m *AcceptRequest) GetType() uint32 { return CONSENSUS_ACCEPT_REQUEST }

type AcceptResponse struct {

}

func (m *AcceptResponse) Serialize(buf *bufio.Writer) error {
	return nil
}

func (m *AcceptResponse) Deserialize(buf *bufio.Reader) error {
	return nil
}

func (m *AcceptResponse) GetType() uint32 { return CONSENSUS_ACCEPT_RESPONSE }

// ----------- encoding helpers -----------

func serializeCommand(cmd *Command, buf *bufio.Writer) error {
	if err := writeFieldString(buf, string(cmd.ID.LeaderID)); err != nil { return err }
	if err := binary.Write(buf, binary.LittleEndian, &cmd.ID.Ballot); err != nil { return err }
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

	return nil
}

func deserializeCommand(buf *bufio.Reader) (*Command, error) {
	cmd := &Command{}
	var err error
	if str, err := readFieldString(buf); err != nil {
		return nil, err
	} else {
		cmd.ID.LeaderID = NodeId(str)
	}
	if err := binary.Read(buf, binary.LittleEndian, &cmd.ID.Ballot); err != nil { return nil, err }

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

	return cmd, nil
}

