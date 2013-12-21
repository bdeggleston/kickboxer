package cluster

import (
	"bufio"
	"encoding/binary"
	"time"
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


