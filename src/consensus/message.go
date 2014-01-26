package consensus

import (
	"bufio"
	"encoding/binary"
)

import (
	"message"
	"serializer"
)

type ScopedMessage interface {
	message.Message
	GetScope() string
}

type BallotMessage interface {
	message.Message
	GetBallot() uint32
}

const (
	MESSAGE_PREACCEPT_REQUEST = uint32(1001)
	MESSAGE_PREACCEPT_RESPONSE = uint32(1002)

	MESSAGE_ACCEPT_REQUEST = uint32(1003)
	MESSAGE_ACCEPT_RESPONSE = uint32(1004)

	MESSAGE_COMMIT_REQUEST = uint32(1005)
	MESSAGE_COMMIT_RESPONSE = uint32(1006)

	MESSAGE_PREPARE_REQUEST = uint32(1007)
	MESSAGE_PREPARE_RESPONSE = uint32(1008)
)

// cheats the message interface implementation
// TODO: actually implement the message interface
type messageCheat struct{}

func (m *messageCheat) Serialize(*bufio.Writer) error   { return nil }
func (m *messageCheat) Deserialize(*bufio.Reader) error { return nil }
func (m *messageCheat) GetType() uint32                 { return 0 }

type PreAcceptRequest struct {

	// the scope name the message
	// is going to
	Scope string

	Instance *Instance
}

func (m *PreAcceptRequest) GetScope() string { return m.Scope }
func (m *PreAcceptRequest) GetType() uint32 { return MESSAGE_PREACCEPT_REQUEST }

func (m *PreAcceptRequest) Serialize(buf *bufio.Writer) error   {
	if err := serializer.WriteFieldString(buf, m.Scope); err != nil { return err }
	if err := instanceLimitedSerialize(m.Instance, buf); err != nil { return err }
	return nil
}

func (m *PreAcceptRequest) Deserialize(buf *bufio.Reader) error {
	if val, err := serializer.ReadFieldString(buf); err != nil { return err } else {
		m.Scope = val
	}
	if val, err := instanceLimitedDeserialize(buf); err != nil { return err } else {
		m.Instance = val
	}
	return nil
}

type PreAcceptResponse struct {

	// indicates the remote node ignored the
	// preaccept due to an out of date ballot
	Accepted bool

	// the highest ballot that's been seen
	// for this instance
	MaxBallot uint32

	// the instance info the remote node added
	// to it's instances. Sequence and Dependencies
	// may be different
	Instance *Instance

	// if the command leader seems to be missing
	// instances in it's deps, return them here
	MissingInstances []*Instance
}

func (m *PreAcceptResponse) GetBallot() uint32 { return m.MaxBallot }
func (m *PreAcceptResponse) GetType() uint32 { return MESSAGE_PREACCEPT_RESPONSE }

func (m *PreAcceptResponse) Serialize(buf *bufio.Writer) error   {
	var accepted byte
	if m.Accepted { accepted = 0xff }
	if err := binary.Write(buf, binary.LittleEndian, &accepted); err != nil { return err }
	if err := binary.Write(buf, binary.LittleEndian, &m.MaxBallot); err != nil { return err }

	if err := instanceLimitedSerialize(m.Instance, buf); err != nil { return err }

	numInst := uint32(len(m.MissingInstances))
	if err := binary.Write(buf, binary.LittleEndian, &numInst); err != nil { return err }
	for _, inst := range m.MissingInstances {
		if err := instanceLimitedSerialize(inst, buf); err != nil { return err }
	}
	return nil
}

func (m *PreAcceptResponse) Deserialize(buf *bufio.Reader) error {
	var accepted byte
	if err := binary.Read(buf, binary.LittleEndian, &accepted); err != nil { return err }
	m.Accepted = accepted != 0x0
	if err := binary.Read(buf, binary.LittleEndian, &m.MaxBallot); err != nil { return err }
	if val, err := instanceLimitedDeserialize(buf); err != nil { return err } else {
		m.Instance = val
	}
	var numInst uint32
	if err := binary.Read(buf, binary.LittleEndian, &numInst); err != nil { return err }
	m.MissingInstances = make([]*Instance, numInst)
	for i := range m.MissingInstances {
		if val, err := instanceLimitedDeserialize(buf); err != nil { return err } else {
			m.MissingInstances[i] = val
		}
	}
	return nil
}

type AcceptRequest struct {

	// the scope name the message
	// is going to
	Scope string

	// the instance the remote node is instructed
	// to accept
	Instance *Instance

	// if the replica seems to be missing
	// instances in it's deps, send them here
	MissingInstances []*Instance
}

func (m *AcceptRequest) GetScope() string { return m.Scope }
func (m *AcceptRequest) GetType() uint32 { return MESSAGE_ACCEPT_REQUEST }

func (m *AcceptRequest) Serialize(buf *bufio.Writer) error   {
	if err := serializer.WriteFieldString(buf, m.Scope); err != nil { return err }

	if err := instanceLimitedSerialize(m.Instance, buf); err != nil { return err }

	numInst := uint32(len(m.MissingInstances))
	if err := binary.Write(buf, binary.LittleEndian, &numInst); err != nil { return err }
	for _, inst := range m.MissingInstances {
		if err := instanceLimitedSerialize(inst, buf); err != nil { return err }
	}
	return nil
}

func (m *AcceptRequest) Deserialize(buf *bufio.Reader) error {
	if val, err := serializer.ReadFieldString(buf); err != nil { return err } else {
		m.Scope = val
	}
	if val, err := instanceLimitedDeserialize(buf); err != nil { return err } else {
		m.Instance = val
	}
	var numInst uint32
	if err := binary.Read(buf, binary.LittleEndian, &numInst); err != nil { return err }
	m.MissingInstances = make([]*Instance, numInst)
	for i := range m.MissingInstances {
		if val, err := instanceLimitedDeserialize(buf); err != nil { return err } else {
			m.MissingInstances[i] = val
		}
	}
	return nil
}

type AcceptResponse struct {
	messageCheat

	// indicates the remote node ignored the
	// preaccept due to an out of date ballot
	Accepted bool

	// the highest ballot that's been seen
	// for this instance
	MaxBallot uint32
}

func (m *AcceptResponse) GetBallot() uint32 { return m.MaxBallot }
func (m *AcceptResponse) GetType() uint32 { return MESSAGE_ACCEPT_RESPONSE }

func (m *AcceptResponse) Serialize(buf *bufio.Writer) error   {
	var accepted byte
	if m.Accepted { accepted = 0xff }
	if err := binary.Write(buf, binary.LittleEndian, &accepted); err != nil { return err }
	if err := binary.Write(buf, binary.LittleEndian, &m.MaxBallot); err != nil { return err }
	return nil
}

func (m *AcceptResponse) Deserialize(buf *bufio.Reader) error {
	var accepted byte
	if err := binary.Read(buf, binary.LittleEndian, &accepted); err != nil { return err }
	m.Accepted = accepted != 0x0
	if err := binary.Read(buf, binary.LittleEndian, &m.MaxBallot); err != nil { return err }
	return nil
}

type CommitRequest struct {
	// the scope name the message
	// is going to
	Scope string

	// the instance the remote node is instructed
	// to accept
	Instance *Instance
}

func (m *CommitRequest) GetScope() string { return m.Scope }
func (m *CommitRequest) GetType() uint32 { return MESSAGE_COMMIT_REQUEST }

func (m *CommitRequest) Serialize(buf *bufio.Writer) error   {
	if err := serializer.WriteFieldString(buf, m.Scope); err != nil { return err }
	if err := instanceLimitedSerialize(m.Instance, buf); err != nil { return err }
	return nil
}

func (m *CommitRequest) Deserialize(buf *bufio.Reader) error {
	if val, err := serializer.ReadFieldString(buf); err != nil { return err } else {
		m.Scope = val
	}
	if val, err := instanceLimitedDeserialize(buf); err != nil { return err } else {
		m.Instance = val
	}
	return nil
}

type CommitResponse struct {}
func (m *CommitResponse) GetType() uint32 { return MESSAGE_COMMIT_RESPONSE }

func (m *CommitResponse) Serialize(buf *bufio.Writer) error   {
	return nil
}

func (m *CommitResponse) Deserialize(buf *bufio.Reader) error {
	return nil
}

type PrepareRequest struct {
	// the scope name the message
	// is going to
	Scope string

	Ballot uint32

	InstanceID InstanceID
}

func (m *PrepareRequest) GetType() uint32 { return MESSAGE_PREPARE_REQUEST }

func (m *PrepareRequest) Serialize(buf *bufio.Writer) error   {
	if err := serializer.WriteFieldString(buf, m.Scope); err != nil { return err }
	if err := binary.Write(buf, binary.LittleEndian, &m.Ballot); err != nil { return err }
	if err := serializer.WriteFieldString(buf, string(m.InstanceID)); err != nil { return err }
	return nil
}

func (m *PrepareRequest) Deserialize(buf *bufio.Reader) error {
	if val, err := serializer.ReadFieldString(buf); err != nil { return err } else {
		m.Scope = val
	}
	if err := binary.Read(buf, binary.LittleEndian, &m.Ballot); err != nil { return err }
	if val, err := serializer.ReadFieldString(buf); err != nil { return err } else {
		m.InstanceID = InstanceID(val)
	}
	return nil
}

type PrepareResponse struct {
	// indicates the remote node ignored the
	// preaccept due to an out of date ballot
	Accepted bool

	Instance *Instance
}

func (m *PrepareResponse) GetType() uint32 { return MESSAGE_PREPARE_RESPONSE }

func (m *PrepareResponse) Serialize(buf *bufio.Writer) error   {
	var accepted byte
	if m.Accepted { accepted = 0xff }
	if err := binary.Write(buf, binary.LittleEndian, &accepted); err != nil { return err }
	if err := instanceLimitedSerialize(m.Instance, buf); err != nil { return err }
	return nil
}

func (m *PrepareResponse) Deserialize(buf *bufio.Reader) error {
	var accepted byte
	if err := binary.Read(buf, binary.LittleEndian, &accepted); err != nil { return err }
	m.Accepted = accepted != 0x0
	if val, err := instanceLimitedDeserialize(buf); err != nil { return err } else {
		m.Instance = val
	}
	return nil
}

func init() {
	message.RegisterMessage(MESSAGE_PREACCEPT_REQUEST, func() message.Message { return &PreAcceptRequest{} })
	message.RegisterMessage(MESSAGE_PREACCEPT_RESPONSE, func() message.Message { return &PreAcceptResponse{} })

	message.RegisterMessage(MESSAGE_ACCEPT_REQUEST, func() message.Message { return &AcceptRequest{} })
	message.RegisterMessage(MESSAGE_ACCEPT_RESPONSE, func() message.Message { return &AcceptResponse{} })

	message.RegisterMessage(MESSAGE_COMMIT_REQUEST, func() message.Message { return &CommitRequest{} })
	message.RegisterMessage(MESSAGE_COMMIT_RESPONSE, func() message.Message { return &CommitResponse{} })

	message.RegisterMessage(MESSAGE_PREPARE_REQUEST, func() message.Message { return &PrepareRequest{} })
	message.RegisterMessage(MESSAGE_PREPARE_RESPONSE, func() message.Message { return &PrepareResponse{} })
}
