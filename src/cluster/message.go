/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/4/13
 * Time: 7:09 AM
 * To change this template use File | Settings | File Templates.
 */
package cluster

import (
	"fmt"
	"bufio"
	"encoding/binary"
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

// sent when connecting to another node
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
	if len(b) != 16 {
		return NewMessageEncodingError(fmt.Sprintf("expected 16 bytes for NodeId, got %v (%v)", b, len(b)))
	}
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
	if len(b) != 16 {
		return NewMessageEncodingError(fmt.Sprintf("expected 16 bytes for Token, got %v (%v)", b, len(b)))
	}
	m.Token = Token(b)

	return nil
}

type ConnectionRequest struct {
	PeerData
}

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
	if len(b) != 16 {
		return NewMessageEncodingError(fmt.Sprintf("expected 16 bytes for NodeId, got %v (%v)", b, len(b)))
	}
	m.NodeId = NodeId(b)

	b, err = readFieldBytes(buf)
	if err != nil { return nil }
	m.Name = string(b)

	b, err = readFieldBytes(buf)
	if err != nil { return nil }
	if len(b) != 16 {
		return NewMessageEncodingError(fmt.Sprintf("expected 16 bytes for Token, got %v (%v)", b, len(b)))
	}
	m.Token = Token(b)

	return nil
}


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
	if len(b) != 16 {
		return NewMessageEncodingError(fmt.Sprintf("expected 16 bytes for NodeId, got %v (%v)", b, len(b)))
	}
	m.NodeId = NodeId(b)
	return nil
}


type DiscoverPeerResponse struct {
	//
	Peers []*PeerData
}

func (m *DiscoverPeerResponse) Serialize(buf *bufio.Writer) error {
	numArgs := uint32(len(m.Peers))
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
