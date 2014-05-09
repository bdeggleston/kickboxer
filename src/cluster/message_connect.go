package cluster

import (
	"bufio"
	"encoding/binary"
	"fmt"
)

import (
	"message"
	"node"
	"serializer"
)

const (
	CONNECTION_REQUEST = uint32(101)
	CONNECTION_ACCEPTED_RESPONSE = uint32(102)
	CONNECTION_REFUSED_RESPONSE = uint32(103)

	DISCOVER_PEERS_REQUEST = uint32(201)
	DISCOVER_PEERS_RESPONSE = uint32(202)
)

// ----------- startup and connection -----------

type PeerData struct {
	// the id of the peer
	NodeId node.NodeId
	// the name of the datacenter the peer belongs to
	DCId DatacenterId
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
	if err := serializer.WriteFieldBytes(buf, []byte(m.NodeId)); err != nil { return err }
	// DCId
	if err := serializer.WriteFieldBytes(buf, []byte(m.DCId)); err != nil { return err }
	// Addr
	if err := serializer.WriteFieldBytes(buf, []byte(m.Addr)); err != nil { return err }
	// Name
	if err := serializer.WriteFieldBytes(buf, []byte(m.Name)); err != nil { return err }
	// Token
	if err := serializer.WriteFieldBytes(buf, []byte(m.Token)); err != nil { return err }

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
	b, err = serializer.ReadFieldBytes(buf)
	if err != nil { return err }
	m.NodeId = NodeId(b)

	// DCId
	b, err = serializer.ReadFieldBytes(buf)
	if err != nil { return err }
	m.DCId = DatacenterId(b)

	// Addr
	b, err = serializer.ReadFieldBytes(buf)
	if err != nil { return err }
	m.Addr = string(b)

	// Name
	b, err = serializer.ReadFieldBytes(buf)
	if err != nil { return err }
	m.Name = string(b)

	// Token
	b, err = serializer.ReadFieldBytes(buf)
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

var _ = message.Message(&ConnectionRequest{})

func (m *ConnectionRequest) GetType() uint32 { return CONNECTION_REQUEST }

// TODO: implement and fix
func (m *ConnectionRequest) NumBytes() int { return 0 }

type ConnectionAcceptedResponse struct {
	// the id of the requesting node
	NodeId node.NodeId
	// the name of the datacenter the peer belongs to
	DCId DatacenterId
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
	if err := serializer.WriteFieldBytes(buf, []byte(m.NodeId)); err != nil { return err }
	if err := serializer.WriteFieldBytes(buf, []byte(m.DCId)); err != nil { return err }
	if err := serializer.WriteFieldBytes(buf, []byte(m.Name)); err != nil { return err }
	if err := serializer.WriteFieldBytes(buf, []byte(m.Token)); err != nil { return err }

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

	b, err = serializer.ReadFieldBytes(buf)
	if err != nil { return nil }
	m.NodeId = node.NodeId(b)

	b, err = serializer.ReadFieldBytes(buf)
	if err != nil { return nil }
	m.DCId = DatacenterId(b)

	b, err = serializer.ReadFieldBytes(buf)
	if err != nil { return nil }
	m.Name = string(b)

	b, err = serializer.ReadFieldBytes(buf)
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

var _ = message.Message(&ConnectionRefusedResponse{})

func (m *ConnectionRefusedResponse) Serialize(buf *bufio.Writer) error {
	return serializer.WriteFieldBytes(buf, []byte(m.Reason))
}

func (m *ConnectionRefusedResponse) Deserialize(buf *bufio.Reader) error {
	if b, err := serializer.ReadFieldBytes(buf); err != nil { return err } else {
		m.Reason = string(b)
	}
	return nil
}

func (m *ConnectionRefusedResponse) GetType() uint32 { return CONNECTION_REFUSED_RESPONSE }

// TODO: implement and fix
func (m *ConnectionRefusedResponse) NumBytes() int { return 0 }

// asks other nodes for peer info
type DiscoverPeersRequest struct {
	// the id of the requesting node
	NodeId node.NodeId
}

var _ = message.Message(&DiscoverPeersRequest{})

func (m *DiscoverPeersRequest) Serialize(buf *bufio.Writer) error {
	// write the number of fields
	numArgs := uint32(1)
	if err := binary.Write(buf, binary.LittleEndian, &numArgs); err != nil { return err }

	// then the fields
	if err := serializer.WriteFieldBytes(buf, []byte(m.NodeId)); err != nil { return err }
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

	b, err = serializer.ReadFieldBytes(buf)
	if err != nil { return nil }
	m.NodeId = node.NodeId(b)
	return nil
}

func (m *DiscoverPeersRequest) GetType() uint32 { return DISCOVER_PEERS_REQUEST }

// TODO: implement and fix
func (m *DiscoverPeersRequest) NumBytes() int { return 0 }

type DiscoverPeerResponse struct {
	//
	Peers []*PeerData
}

var _ = message.Message(&DiscoverPeerResponse{})

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


