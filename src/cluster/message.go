package cluster

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const (
	CONNECTION_REQUEST = uint32(101)
	CONNECTION_ACCEPTED_RESPONSE = uint32(102)
	CONNECTION_REFUSED_RESPONSE = uint32(103)

	DISCOVER_PEERS_REQUEST = uint32(201)
	DISCOVER_PEERS_RESPONSE = uint32(202)

	READ_REQUEST = uint32(301)
	WRITE_REQUEST = uint32(302)
	QUERY_RESPONSE = uint32(303)

	STREAM_REQUEST = uint32(401)
	STREAM_RESPONSE = uint32(402)
	STREAM_COMPLETE_REQUEST = uint32(403)
	STREAM_COMPLETE_RESPONSE = uint32(404)
	STREAM_DATA_REQUEST = uint32(405)
	STREAM_DATA_RESPONSE = uint32(406)

	//internal messages
	close_connection = uint32(0xffffff01)
)

type MessageError struct {
	reason string
}

func (e *MessageError) Error() string {
	return e.reason
}

type MessageEncodingError struct {
	MessageError
}

func NewMessageEncodingError(reason string) *MessageEncodingError {
	return &MessageEncodingError{MessageError{reason}}
}

// message wire protocol is as follow:
// [type (4b)][num fields (4b)]...[field size (4b)][field data]
// each message type can define the data
// format as needed
type Message interface {

	// serializes the entire message, including
	// headers. Pass a wrapped connection in to send
	Serialize(*bufio.Writer) error

	// deserializes everything after the size data
	// pass in a wrapped exception to receive
	Deserialize(*bufio.Reader) error

	// returns the message type enum
	GetType() uint32
}

func GetMessageType(buf *bufio.Reader) (mtype uint32, err error) {
	err = binary.Read(buf, binary.BigEndian, &mtype)
	return
}

// writes the field length, then the field to the writer
func writeFieldBytes(buf *bufio.Writer, bytes []byte) error {
	//write field length
	size := uint32(len(bytes))
	if err := binary.Write(buf, binary.LittleEndian, &size); err != nil {
		return err
	}
	// write field
	n, err := buf.Write(bytes);
	if err != nil {
		return err
	}
	if uint32(n) != size {
		return NewMessageEncodingError(fmt.Sprintf("unexpected num bytes written. Expected %v, got %v", size, n))
	}
	return nil
}

// read field bytes
func readFieldBytes(buf *bufio.Reader) ([]byte, error) {
	var size uint32
	if err := binary.Read(buf, binary.LittleEndian, &size); err != nil {
		return nil, err
	}

	bytes := make([]byte, size)
	n, err := buf.Read(bytes)
	if err != nil {
		return nil, err
	}
	if uint32(n) != size {
		return nil, NewMessageEncodingError(fmt.Sprintf("unexpected num bytes read. Expected %v, got %v", size, n))
	}
	return bytes, nil
}

// ----------- startup and connection -----------

type PeerData struct {
	// the id of the requesting node
	NodeId NodeId
	// the address of the requesting node
	Addr string
	// the name of the requesting node
	Name string
	// the token of the requesting node
	Token Token
}

func (m *PeerData) Serialize(buf *bufio.Writer) error {

	// write the number of fields
	numArgs := uint32(4)
	if err := binary.Write(buf, binary.LittleEndian, &numArgs); err != nil { return err }

	// then the fields
	// NodeId
	if err := writeFieldBytes(buf, []byte(m.NodeId)); err != nil { return err }
	// Addr
	if err := writeFieldBytes(buf, []byte(m.Addr)); err != nil { return err }
	// Name
	if err := writeFieldBytes(buf, []byte(m.Name)); err != nil { return err }
	// Token
	if err := writeFieldBytes(buf, []byte(m.Token)); err != nil { return err }

	return nil
}

func (m *PeerData) Deserialize(buf *bufio.Reader) error {

	// check the number of fields
	var numFields uint32
	if err := binary.Read(buf, binary.LittleEndian, &numFields); err != nil { return err }
	if numFields != 4 {
		return NewMessageEncodingError(fmt.Sprintf("unexpected num fields received. Expected %v, got %v", 3, numFields))
	}

	// get the fields
	var b []byte
	var err error

	// NodeId
	b, err = readFieldBytes(buf)
	if err != nil { return err }
	m.NodeId = NodeId(b)

	// Addr
	b, err = readFieldBytes(buf)
	if err != nil { return err }
	m.Addr = string(b)

	// Name
	b, err = readFieldBytes(buf)
	if err != nil { return err }
	m.Name = string(b)

	// Token
	b, err = readFieldBytes(buf)
	if err != nil { return err }
	if len(b) < 1 {
		return NewMessageEncodingError(fmt.Sprintf("expected at least one byte for Token, got %v (%v)", b, len(b)))
	}
	m.Token = Token(b)

	return nil
}

// sent when connecting to another node
type ConnectionRequest struct {
	PeerData
}

func (m *ConnectionRequest) GetType() uint32 { return CONNECTION_REQUEST }

type ConnectionAcceptedResponse struct {
	// the id of the requesting node
	NodeId NodeId
	// the name of the requesting node
	Name string
	// the token of the requesting node
	Token Token
}


func (m *ConnectionAcceptedResponse) Serialize(buf *bufio.Writer) error {

	// write the number of fields
	numArgs := uint32(3)
	if err := binary.Write(buf, binary.LittleEndian, &numArgs); err != nil { return err }

	// then the fields
	if err := writeFieldBytes(buf, []byte(m.NodeId)); err != nil { return err }
	if err := writeFieldBytes(buf, []byte(m.Name)); err != nil { return err }
	if err := writeFieldBytes(buf, []byte(m.Token)); err != nil { return err }

	return nil
}

func (m *ConnectionAcceptedResponse) Deserialize(buf *bufio.Reader) error {

	// check the number of fields
	var numFields uint32
	if err := binary.Read(buf, binary.LittleEndian, &numFields); err != nil { return err }
	if numFields != 3 {
		return NewMessageEncodingError(fmt.Sprintf("unexpected num fields received. Expected %v, got %v", 3, numFields))
	}

	// get the fields
	var b []byte
	var err error

	b, err = readFieldBytes(buf)
	if err != nil { return nil }
	m.NodeId = NodeId(b)

	b, err = readFieldBytes(buf)
	if err != nil { return nil }
	m.Name = string(b)

	b, err = readFieldBytes(buf)
	if err != nil { return nil }
	if len(b) < 1 {
		return NewMessageEncodingError(fmt.Sprintf("expected at least 1 byte for Token, got %v (%v)", b, len(b)))
	}
	m.Token = Token(b)

	return nil
}

func (m *ConnectionAcceptedResponse) GetType() uint32 { return CONNECTION_ACCEPTED_RESPONSE }


type ConnectionRefusedResponse struct {
	Reason string
}

func (m *ConnectionRefusedResponse) Serialize(buf *bufio.Writer) error {
	return writeFieldBytes(buf, []byte(m.Reason))
}

func (m *ConnectionRefusedResponse) Deserialize(buf *bufio.Reader) error {
	if b, err := readFieldBytes(buf); err != nil { return err } else {
		m.Reason = string(b)
	}
	return nil
}

func (m *ConnectionRefusedResponse) GetType() uint32 { return CONNECTION_REFUSED_RESPONSE }

// asks other nodes for peer info
type DiscoverPeersRequest struct {
	// the id of the requesting node
	NodeId NodeId
}

func (m *DiscoverPeersRequest) Serialize(buf *bufio.Writer) error {
	// write the number of fields
	numArgs := uint32(1)
	if err := binary.Write(buf, binary.LittleEndian, &numArgs); err != nil { return err }

	// then the fields
	if err := writeFieldBytes(buf, []byte(m.NodeId)); err != nil { return err }
	return nil
}

func (m *DiscoverPeersRequest) Deserialize(buf *bufio.Reader) error {

	// check the number of fields
	var numFields uint32
	if err := binary.Read(buf, binary.LittleEndian, &numFields); err != nil { return err }
	if numFields != uint32(1) {
		return NewMessageEncodingError(fmt.Sprintf("unexpected num fields received. Expected %v, got %v", 1, numFields))
	}

	// get the fields
	var b []byte
	var err error

	b, err = readFieldBytes(buf)
	if err != nil { return nil }
	m.NodeId = NodeId(b)
	return nil
}

func (m *DiscoverPeersRequest) GetType() uint32 { return DISCOVER_PEERS_REQUEST }

type DiscoverPeerResponse struct {
	//
	Peers []*PeerData
}

func (m *DiscoverPeerResponse) Serialize(buf *bufio.Writer) error {
	var numArgs uint32
	if m.Peers == nil {
		numArgs = uint32(0)
	} else {
		numArgs = uint32(len(m.Peers))
	}
	if err := binary.Write(buf, binary.LittleEndian, &numArgs); err != nil { return err }
	for i:=0; i<int(numArgs); i++ {
		pd := m.Peers[i]
		if err := pd.Serialize(buf); err != nil { return err }
	}

	return nil
}

func (m *DiscoverPeerResponse) Deserialize(buf *bufio.Reader) error {
	// check the number of fields
	var numFields uint32
	if err := binary.Read(buf, binary.LittleEndian, &numFields); err != nil { return err }

	m.Peers = make([]*PeerData, numFields)
	for i:=0; i<int(numFields); i++ {
		pd := &PeerData{}
		if err := pd.Deserialize(buf); err != nil { return err }
		m.Peers[i] = pd
	}
	return nil
}

func (m *DiscoverPeerResponse) GetType() uint32 { return DISCOVER_PEERS_RESPONSE }

// ----------- query execution -----------

type ReadRequest struct {
	Cmd string
	Key string
	Args []string
}

func (m *ReadRequest) Serialize(buf *bufio.Writer) error {
	if err := writeFieldBytes(buf, []byte(m.Cmd)); err != nil { return err }
	if err := writeFieldBytes(buf, []byte(m.Key)); err != nil { return err }
	numArgs := uint32(len(m.Args))
	if err := binary.Write(buf, binary.LittleEndian, &numArgs); err != nil { return err }
	for i:=0;i<int(numArgs);i++ {
		if err := writeFieldBytes(buf, []byte(m.Args[i])); err != nil { return err }
	}
	return nil
}

func (m *ReadRequest) Deserialize(buf *bufio.Reader) error {
	if b, err := readFieldBytes(buf); err != nil { return err } else {
		m.Cmd = string(b)
	}
	if b, err := readFieldBytes(buf); err != nil { return err } else {
		m.Key = string(b)
	}

	var numArgs uint32
	if err := binary.Read(buf, binary.LittleEndian, &numArgs); err != nil { return err }
	m.Args = make([]string, numArgs)
	for i:=0;i<int(numArgs);i++ {
		if b, err := readFieldBytes(buf); err != nil { return err } else {
			m.Args[i] = string(b)
		}
	}

	return nil
}

func (m *ReadRequest) GetType() uint32 { return READ_REQUEST }

type WriteRequest struct {
	ReadRequest
	Timestamp time.Time
}

func (m *WriteRequest) Serialize(buf *bufio.Writer) error {
	if err := m.ReadRequest.Serialize(buf); err != nil { return err }
	if b, err := m.Timestamp.GobEncode(); err != nil { return err } else {
		if err := writeFieldBytes(buf, b); err != nil { return err }
	}
	return nil
}

func (m *WriteRequest) Deserialize(buf *bufio.Reader) error {
	if err := m.ReadRequest.Deserialize(buf); err != nil { return err }
	if b, err := readFieldBytes(buf); err != nil { return err } else  {
		if err := m.Timestamp.GobDecode(b); err != nil { return err }
	}
	return nil
}

func (m *WriteRequest) GetType() uint32 { return WRITE_REQUEST }

type QueryResponse struct {
	// ad hoc data returned by the storage backend
	Data [][]byte
}

func (m *QueryResponse) Serialize(buf *bufio.Writer) error {
	size := uint32(len(m.Data))
	if err := binary.Write(buf, binary.LittleEndian, &size); err != nil { return err }
	for i:=0;i<int(size);i++ {
		if err := writeFieldBytes(buf, m.Data[i]); err != nil { return err }
	}
	return nil
}

func (m *QueryResponse) Deserialize(buf *bufio.Reader) error {
	var size uint32
	if err := binary.Read(buf, binary.LittleEndian, &size); err != nil { return err }

	m.Data = make([][]byte, size)
	for i:=0;i<int(size);i++ {
		if b, err := readFieldBytes(buf); err != nil { return err } else {
			m.Data[i] = b
		}
	}

	return nil
}

func (m *QueryResponse) GetType() uint32 { return QUERY_RESPONSE }

// ----------- streaming messages -----------

// requests that a node initiate streaming to the requesting node
type StreamRequest struct { }
func (m *StreamRequest) Serialize(*bufio.Writer) error { return nil }
func (m *StreamRequest) Deserialize(*bufio.Reader) error { return nil }
func (m *StreamRequest) GetType() uint32 { return STREAM_REQUEST }

// stream request acknowledgement
type StreamResponse struct { }
func (m *StreamResponse) Serialize(*bufio.Writer) error { return nil }
func (m *StreamResponse) Deserialize(*bufio.Reader) error { return nil }
func (m *StreamResponse) GetType() uint32 { return STREAM_RESPONSE }

// notifies a node that there is no more data to stream to it
type StreamCompleteRequest struct { }
func (m *StreamCompleteRequest) Serialize(*bufio.Writer) error { return nil }
func (m *StreamCompleteRequest) Deserialize(*bufio.Reader) error { return nil }
func (m *StreamCompleteRequest) GetType() uint32 { return STREAM_COMPLETE_REQUEST }

// stream completion request acknowledgement
type StreamCompleteResponse struct { }
func (m *StreamCompleteResponse) Serialize(*bufio.Writer) error { return nil }
func (m *StreamCompleteResponse) Deserialize(*bufio.Reader) error { return nil }
func (m *StreamCompleteResponse) GetType() uint32 { return STREAM_COMPLETE_RESPONSE }

type StreamData struct {
	Key string
	Data []byte
}
// sends arbitrary byte data from one
type StreamDataRequest struct {
	Data []StreamData
}

type StreamDataResponse struct {}

// ----------- read / write functions -----------

// writes a message to the given writer
func WriteMessage(buf io.Writer, m Message) error {
	writer := bufio.NewWriter(buf)

	// write the message type
	mtype := m.GetType()
	if err:= binary.Write(writer, binary.LittleEndian, &mtype); err != nil { return err }
	if err := m.Serialize(writer); err != nil { return err }
	if err := writer.Flush(); err != nil { return err }
	return nil
}

// reads a message from the given reader
func ReadMessage(buf io.Reader) (Message, uint32, error) {
	reader := bufio.NewReader(buf)
	var mtype uint32
	if err := binary.Read(reader, binary.LittleEndian, &mtype); err != nil { return nil, 0, err }

	var msg Message
	switch mtype {
	case CONNECTION_REQUEST:
		msg = &ConnectionRequest{}
	case CONNECTION_ACCEPTED_RESPONSE:
		msg = &ConnectionAcceptedResponse{}
	case CONNECTION_REFUSED_RESPONSE:
		msg = &ConnectionRefusedResponse{}
	case DISCOVER_PEERS_REQUEST:
		msg = &DiscoverPeersRequest{}
	case DISCOVER_PEERS_RESPONSE:
		msg = &DiscoverPeerResponse{}
	case READ_REQUEST:
		msg = &ReadRequest{}
	case WRITE_REQUEST:
		msg = &WriteRequest{}
	case QUERY_RESPONSE:
		msg = &QueryResponse{}
	case STREAM_REQUEST:
		msg = &StreamRequest{}
	case STREAM_RESPONSE:
		msg = &StreamResponse{}
	case STREAM_COMPLETE_REQUEST:
		msg = &StreamCompleteRequest{}
	case STREAM_COMPLETE_RESPONSE:
		msg = &StreamCompleteResponse{}
	case close_connection:
		msg = &closeConnection{}
	default:
		return nil, 0, NewMessageEncodingError(fmt.Sprintf("Unexpected message type: %v", mtype))
	}

	if err := msg.Deserialize(reader); err != nil { return nil, 0, err }
	return msg, mtype, nil
}

// ----------- internal messages -----------

// message used for testing purposes to close
// a connection
type closeConnection struct { }
func (m *closeConnection) Serialize(_ *bufio.Writer) error { return nil }
func (m *closeConnection) Deserialize(_ *bufio.Reader) error { return nil }
func (m *closeConnection) GetType() uint32 { return close_connection }
