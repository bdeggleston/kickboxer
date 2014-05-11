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
	"types"
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
	// NodeId
	if err := (&m.NodeId).WriteBuffer(buf); err != nil { return err }
	// DCId
	if err := serializer.WriteFieldString(buf, string(m.DCId)); err != nil { return err }
	// Addr
	if err := serializer.WriteFieldString(buf, string(m.Addr)); err != nil { return err }
	// Name
	if err := serializer.WriteFieldString(buf, string(m.Name)); err != nil { return err }
	// Token
	if err := serializer.WriteFieldBytes(buf, []byte(m.Token)); err != nil { return err }

	return nil
}

func (m *PeerData) Deserialize(buf *bufio.Reader) error {
	// get the fields
	var b []byte
	var err error

	// NodeId
	if err := (&m.NodeId).ReadBuffer(buf); err != nil { return err }

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

func (m *PeerData) NumBytes() int {
	numBytes := 0

	// node id
	numBytes += types.UUID_NUM_BYTES

	// datacenter id
	numBytes += serializer.NumStringBytes(string(m.DCId))

	// address
	numBytes += serializer.NumStringBytes(m.Addr)

	// name
	numBytes += serializer.NumStringBytes(m.Name)

	// token
	numBytes += serializer.NumSliceBytes(m.Token)

	return numBytes
}

// sent when connecting to another node
type ConnectionRequest struct {
	PeerData
}

var _ = message.Message(&ConnectionRequest{})

func (m *ConnectionRequest) GetType() uint32 { return CONNECTION_REQUEST }

func (m *ConnectionRequest) NumBytes() int { return m.PeerData.NumBytes() }

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

	// then the fields
	if err := (&m.NodeId).WriteBuffer(buf); err != nil { return err }
	if err := serializer.WriteFieldBytes(buf, []byte(m.DCId)); err != nil { return err }
	if err := serializer.WriteFieldBytes(buf, []byte(m.Name)); err != nil { return err }
	if err := serializer.WriteFieldBytes(buf, []byte(m.Token)); err != nil { return err }

	return nil
}

func (m *ConnectionAcceptedResponse) Deserialize(buf *bufio.Reader) error {
	var b []byte
	var err error

	if err := (&m.NodeId).ReadBuffer(buf); err != nil { return err }

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

func (m *ConnectionAcceptedResponse) NumBytes() int {
	numBytes := 0

	// node id
	numBytes += types.UUID_NUM_BYTES

	// datacenter id
	numBytes += serializer.NumStringBytes(string(m.DCId))

	// name
	numBytes += serializer.NumStringBytes(m.Name)

	// token
	numBytes += serializer.NumSliceBytes(m.Token)

	return numBytes
}


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

func (m *ConnectionRefusedResponse) NumBytes() int {
	return serializer.NumStringBytes(m.Reason)
}

// asks other nodes for peer info
type DiscoverPeersRequest struct {
	// the id of the requesting node
	NodeId node.NodeId
}

var _ = message.Message(&DiscoverPeersRequest{})

func (m *DiscoverPeersRequest) Serialize(buf *bufio.Writer) error {
	// then the fields
	if err := (&m.NodeId).WriteBuffer(buf); err != nil { return err }
	return nil
}

func (m *DiscoverPeersRequest) Deserialize(buf *bufio.Reader) error {
	if err := (&m.NodeId).ReadBuffer(buf); err != nil { return err }
	return nil
}

func (m *DiscoverPeersRequest) GetType() uint32 { return DISCOVER_PEERS_REQUEST }

func (m *DiscoverPeersRequest) NumBytes() int { return types.UUID_NUM_BYTES }

type DiscoverPeerResponse struct {
	//
	Peers []*PeerData
}

var _ = message.Message(&DiscoverPeerResponse{})

func (m *DiscoverPeerResponse) Serialize(buf *bufio.Writer) error {
	numPeers := uint32(len(m.Peers))
	if err := binary.Write(buf, binary.LittleEndian, &numPeers); err != nil { return err }
	for i:=0; i<int(numPeers); i++ {
		pd := m.Peers[i]
		if err := pd.Serialize(buf); err != nil { return err }
	}

	return nil
}

func (m *DiscoverPeerResponse) Deserialize(buf *bufio.Reader) error {
	var numPeers uint32
	if err := binary.Read(buf, binary.LittleEndian, &numPeers); err != nil { return err }

	m.Peers = make([]*PeerData, numPeers)
	for i:=0; i<int(numPeers); i++ {
		pd := &PeerData{}
		if err := pd.Deserialize(buf); err != nil { return err }
		m.Peers[i] = pd
	}
	return nil
}

func (m *DiscoverPeerResponse) GetType() uint32 { return DISCOVER_PEERS_RESPONSE }

// TODO: implement and fix
func (m *DiscoverPeerResponse) NumBytes() int {
	numBytes := 0

	// num peers
	numBytes += 4

	for _, peer := range m.Peers {
		numBytes += peer.NumBytes()
	}
	return numBytes
}


func init() {
	message.RegisterMessage(CONNECTION_REQUEST, func() message.Message {return &ConnectionRequest{}} )
	message.RegisterMessage(CONNECTION_ACCEPTED_RESPONSE, func() message.Message {return &ConnectionAcceptedResponse{}} )
	message.RegisterMessage(CONNECTION_REFUSED_RESPONSE, func() message.Message {return &ConnectionRefusedResponse{}} )

	message.RegisterMessage(DISCOVER_PEERS_REQUEST, func() message.Message {return &DiscoverPeersRequest{}} )
	message.RegisterMessage(DISCOVER_PEERS_RESPONSE, func() message.Message {return &DiscoverPeerResponse{}} )
}
