package consensus

import (
	"bufio"
	"encoding/binary"
)

import (
	"message"
)

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

	MESSAGE_PREPARE_SUCCESSOR_REQUEST = uint32(1009)
	MESSAGE_PREPARE_SUCCESSOR_RESPONSE = uint32(1010)

	MESSAGE_INSTANCE_REQUEST = uint32(1011)
	MESSAGE_INSTANCE_RESPONSE = uint32(1012)
)

type PreAcceptRequest struct {
	Instance *Instance
}

func (m *PreAcceptRequest) GetType() uint32 { return MESSAGE_PREACCEPT_REQUEST }

func (m *PreAcceptRequest) Serialize(buf *bufio.Writer) error   {
	if err := instanceLimitedSerialize(m.Instance, buf); err != nil { return err }
	return nil
}

func (m *PreAcceptRequest) Deserialize(buf *bufio.Reader) error {
	if val, err := instanceLimitedDeserialize(buf); err != nil { return err } else {
		m.Instance = val
	}
	return nil
}

var _ = &PreAcceptRequest{}

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

var _ = &PreAcceptResponse{}

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

	// the instance the remote node is instructed
	// to accept
	Instance *Instance

	// if the replica seems to be missing
	// instances in it's deps, send them here
	MissingInstances []*Instance
}

var _ = &AcceptRequest{}

func (m *AcceptRequest) GetType() uint32 { return MESSAGE_ACCEPT_REQUEST }

func (m *AcceptRequest) Serialize(buf *bufio.Writer) error   {
	if err := instanceLimitedSerialize(m.Instance, buf); err != nil { return err }

	numInst := uint32(len(m.MissingInstances))
	if err := binary.Write(buf, binary.LittleEndian, &numInst); err != nil { return err }
	for _, inst := range m.MissingInstances {
		if err := instanceLimitedSerialize(inst, buf); err != nil { return err }
	}
	return nil
}

func (m *AcceptRequest) Deserialize(buf *bufio.Reader) error {
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
	// indicates the remote node ignored the
	// preaccept due to an out of date ballot
	Accepted bool

	// the highest ballot that's been seen
	// for this instance
	MaxBallot uint32
}

var _ = &AcceptResponse{}

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
	// the instance the remote node is instructed
	// to accept
	Instance *Instance
}

var _ = &CommitRequest{}

func (m *CommitRequest) GetType() uint32 { return MESSAGE_COMMIT_REQUEST }

func (m *CommitRequest) Serialize(buf *bufio.Writer) error   {
	if err := instanceLimitedSerialize(m.Instance, buf); err != nil { return err }
	return nil
}

func (m *CommitRequest) Deserialize(buf *bufio.Reader) error {
	if val, err := instanceLimitedDeserialize(buf); err != nil { return err } else {
		m.Instance = val
	}
	return nil
}

type CommitResponse struct {}

var _ = &CommitResponse{}

func (m *CommitResponse) GetType() uint32 { return MESSAGE_COMMIT_RESPONSE }

func (m *CommitResponse) Serialize(buf *bufio.Writer) error   {
	return nil
}

func (m *CommitResponse) Deserialize(buf *bufio.Reader) error {
	return nil
}

type PrepareRequest struct {
	Ballot uint32

	InstanceID InstanceID
}

var _ = &PrepareRequest{}

func (m *PrepareRequest) GetType() uint32 { return MESSAGE_PREPARE_REQUEST }

func (m *PrepareRequest) Serialize(buf *bufio.Writer) error   {
	if err := binary.Write(buf, binary.LittleEndian, &m.Ballot); err != nil { return err }
	if err := (&m.InstanceID).WriteBuffer(buf); err != nil { return err }
	return nil
}

func (m *PrepareRequest) Deserialize(buf *bufio.Reader) error {
	if err := binary.Read(buf, binary.LittleEndian, &m.Ballot); err != nil { return err }
	if err := (&m.InstanceID).ReadBuffer(buf); err != nil { return err }
	return nil
}

type PrepareResponse struct {
	// indicates the remote node ignored the
	// preaccept due to an out of date ballot
	Accepted bool

	Instance *Instance
}

var _ = &PrepareResponse{}

func (m *PrepareResponse) GetType() uint32 { return MESSAGE_PREPARE_RESPONSE }

func (m *PrepareResponse) Serialize(buf *bufio.Writer) error   {
	var accepted byte
	if m.Accepted { accepted = 0xff }
	if err := binary.Write(buf, binary.LittleEndian, &accepted); err != nil { return err }

	var isNil byte
	if m.Instance == nil { isNil = 0xff }
	if err := binary.Write(buf, binary.LittleEndian, &isNil); err != nil { return err }
	if m.Instance != nil {
		if err := instanceLimitedSerialize(m.Instance, buf); err != nil { return err }
	}
	return nil
}

func (m *PrepareResponse) Deserialize(buf *bufio.Reader) error {
	var accepted byte
	if err := binary.Read(buf, binary.LittleEndian, &accepted); err != nil { return err }
	m.Accepted = accepted != 0x0

	var isNil byte
	if err := binary.Read(buf, binary.LittleEndian, &isNil); err != nil { return err }
	if isNil == 0x0 {
		if val, err := instanceLimitedDeserialize(buf); err != nil { return err } else {
			m.Instance = val
		}
	}
	return nil
}

// requests that the prepare successor
// initiate a prepare phase
type PrepareSuccessorRequest struct {
	InstanceID InstanceID
}

var _ = &PrepareSuccessorRequest{}

func (m *PrepareSuccessorRequest) GetType() uint32 { return MESSAGE_PREPARE_SUCCESSOR_REQUEST }

func (m *PrepareSuccessorRequest) Serialize(buf *bufio.Writer) error   {
	if err := (&m.InstanceID).WriteBuffer(buf); err != nil { return err }
	return nil
}

func (m *PrepareSuccessorRequest) Deserialize(buf *bufio.Reader) error {
	if err := (&m.InstanceID).ReadBuffer(buf); err != nil { return err }
	return nil
}

type PrepareSuccessorResponse struct {
	Instance *Instance
}

var _ = &PrepareSuccessorResponse{}

func (m *PrepareSuccessorResponse) GetType() uint32 { return MESSAGE_PREPARE_SUCCESSOR_RESPONSE }

func (m *PrepareSuccessorResponse) Serialize(buf *bufio.Writer) error   {
	var isNil byte
	if m.Instance == nil { isNil = 0xff }
	if err := binary.Write(buf, binary.LittleEndian, &isNil); err != nil { return err }
	if m.Instance != nil {
		if err := instanceLimitedSerialize(m.Instance, buf); err != nil { return err }
	}
	return nil
}

func (m *PrepareSuccessorResponse) Deserialize(buf *bufio.Reader) error {
	var isNil byte
	if err := binary.Read(buf, binary.LittleEndian, &isNil); err != nil { return err }
	if isNil == 0x0 {
		if val, err := instanceLimitedDeserialize(buf); err != nil { return err } else {
			m.Instance = val
		}
	}
	return nil
}

type InstanceRequest struct {
	InstanceIDs []InstanceID
}

var _ = &InstanceRequest{}

func (m *InstanceRequest) GetType() uint32 { return MESSAGE_INSTANCE_REQUEST }

func (m *InstanceRequest) Serialize(buf *bufio.Writer) error   {
	numIds := uint32(len(m.InstanceIDs))
	if err := binary.Write(buf, binary.LittleEndian, &numIds); err != nil { return err }

	for i := range m.InstanceIDs {
		if err := (&m.InstanceIDs[i]).WriteBuffer(buf); err != nil { return err }
	}
	return nil
}

func (m *InstanceRequest) Deserialize(buf *bufio.Reader) error {
	var numIds uint32
	if err := binary.Read(buf, binary.LittleEndian, &numIds); err != nil { return err }
	m.InstanceIDs = make([]InstanceID, numIds)
	for i := range m.InstanceIDs {
		if err := (&m.InstanceIDs[i]).ReadBuffer(buf); err != nil { return err }
	}
	return nil
}

type InstanceResponse struct {
	Instances []*Instance
}

var _ = &InstanceResponse{}

func (m *InstanceResponse) GetType() uint32 { return MESSAGE_INSTANCE_RESPONSE }

func (m *InstanceResponse) Serialize(buf *bufio.Writer) error   {
	numInst := uint32(len(m.Instances))
	if err := binary.Write(buf, binary.LittleEndian, &numInst); err != nil { return err }
	for _, inst := range m.Instances {
		if err := instanceLimitedSerialize(inst, buf); err != nil { return err }
	}
	return nil
}

func (m *InstanceResponse) Deserialize(buf *bufio.Reader) error {
	var numInst uint32
	if err := binary.Read(buf, binary.LittleEndian, &numInst); err != nil { return err }
	m.Instances = make([]*Instance, numInst)
	for i := range m.Instances {
		if val, err := instanceLimitedDeserialize(buf); err != nil { return err } else {
			m.Instances[i] = val
		}
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

	message.RegisterMessage(MESSAGE_PREPARE_SUCCESSOR_REQUEST, func() message.Message { return &PrepareSuccessorRequest{} })
	message.RegisterMessage(MESSAGE_PREPARE_SUCCESSOR_RESPONSE, func() message.Message { return &PrepareSuccessorResponse{} })

	message.RegisterMessage(MESSAGE_INSTANCE_REQUEST, func() message.Message { return &InstanceRequest{} })
	message.RegisterMessage(MESSAGE_INSTANCE_RESPONSE, func() message.Message { return &InstanceResponse{} })
}
