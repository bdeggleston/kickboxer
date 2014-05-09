package cluster

import (
	"bufio"
	"encoding/binary"
	"time"
)

import (
	"message"
	"serializer"
)

const (
	READ_REQUEST = uint32(301)
	WRITE_REQUEST = uint32(302)
	QUERY_RESPONSE = uint32(303)
)

// ----------- query execution -----------

type ReadRequest struct {
	Cmd string
	Key string
	Args []string
}

var _ = message.Message(&ReadRequest{})

func (m *ReadRequest) Serialize(buf *bufio.Writer) error {
	if err := serializer.WriteFieldBytes(buf, []byte(m.Cmd)); err != nil { return err }
	if err := serializer.WriteFieldBytes(buf, []byte(m.Key)); err != nil { return err }
	numArgs := uint32(len(m.Args))
	if err := binary.Write(buf, binary.LittleEndian, &numArgs); err != nil { return err }
	for i:=0;i<int(numArgs);i++ {
		if err := serializer.WriteFieldBytes(buf, []byte(m.Args[i])); err != nil { return err }
	}
	return nil
}

func (m *ReadRequest) Deserialize(buf *bufio.Reader) error {
	if b, err := serializer.ReadFieldBytes(buf); err != nil { return err } else {
		m.Cmd = string(b)
	}
	if b, err := serializer.ReadFieldBytes(buf); err != nil { return err } else {
		m.Key = string(b)
	}

	var numArgs uint32
	if err := binary.Read(buf, binary.LittleEndian, &numArgs); err != nil { return err }
	m.Args = make([]string, numArgs)
	for i:=0;i<int(numArgs);i++ {
		if b, err := serializer.ReadFieldBytes(buf); err != nil { return err } else {
			m.Args[i] = string(b)
		}
	}

	return nil
}

func (m *ReadRequest) GetType() uint32 { return READ_REQUEST }

// TODO: implement and fix
func (m *ReadRequest) NumBytes() int { return 0 }

type WriteRequest struct {
	ReadRequest
	Timestamp time.Time
}

var _ = message.Message(&WriteRequest{})

func (m *WriteRequest) Serialize(buf *bufio.Writer) error {
	if err := m.ReadRequest.Serialize(buf); err != nil { return err }
	if b, err := m.Timestamp.GobEncode(); err != nil { return err } else {
		if err := serializer.WriteFieldBytes(buf, b); err != nil { return err }
	}
	return nil
}

func (m *WriteRequest) Deserialize(buf *bufio.Reader) error {
	if err := m.ReadRequest.Deserialize(buf); err != nil { return err }
	if b, err := serializer.ReadFieldBytes(buf); err != nil { return err } else  {
		if err := m.Timestamp.GobDecode(b); err != nil { return err }
	}
	return nil
}

func (m *WriteRequest) GetType() uint32 { return WRITE_REQUEST }

// TODO: implement and fix
func (m *WriteRequest) NumBytes() int { return 0 }

type QueryResponse struct {
	// ad hoc data returned by the storage backend
	Data [][]byte
}

var _ = message.Message(&QueryResponse{})

func (m *QueryResponse) Serialize(buf *bufio.Writer) error {
	size := uint32(len(m.Data))
	if err := binary.Write(buf, binary.LittleEndian, &size); err != nil { return err }
	for i:=0;i<int(size);i++ {
		if err := serializer.WriteFieldBytes(buf, m.Data[i]); err != nil { return err }
	}
	return nil
}

func (m *QueryResponse) Deserialize(buf *bufio.Reader) error {
	var size uint32
	if err := binary.Read(buf, binary.LittleEndian, &size); err != nil { return err }

	m.Data = make([][]byte, size)
	for i:=0;i<int(size);i++ {
		if b, err := serializer.ReadFieldBytes(buf); err != nil { return err } else {
			m.Data[i] = b
		}
	}

	return nil
}

func (m *QueryResponse) GetType() uint32 { return QUERY_RESPONSE }

// TODO: implement and fix
func (m *QueryResponse) NumBytes() int { return 0 }


