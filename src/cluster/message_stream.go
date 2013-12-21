package cluster

import (
	"bufio"
	"encoding/binary"
)

const (
	STREAM_REQUEST = uint32(401)
	STREAM_RESPONSE = uint32(402)
	STREAM_COMPLETE_REQUEST = uint32(403)
	STREAM_COMPLETE_RESPONSE = uint32(404)
	STREAM_DATA_REQUEST = uint32(405)
	STREAM_DATA_RESPONSE = uint32(406)
)

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

func (m *StreamData) Serialize(buf *bufio.Writer) error {
	if err := writeFieldBytes(buf, []byte(m.Key)); err != nil { return err }
	if err := writeFieldBytes(buf, m.Data); err != nil { return err }
	if err := buf.Flush(); err != nil { return err }
	return nil
}

func (m *StreamData) Deserialize(buf *bufio.Reader) error {
	if b, err := readFieldBytes(buf); err != nil { return err } else {
		m.Key = string(b)
	}
	if b, err := readFieldBytes(buf); err != nil { return err } else {
		m.Data = b
	}
	return nil
}

// sends arbitrary byte data from one
type StreamDataRequest struct {
	Data []*StreamData
}

func (m *StreamDataRequest) Serialize(buf *bufio.Writer) error {
	size := uint32(len(m.Data))
	if err := binary.Write(buf, binary.LittleEndian, &size); err != nil { return err }
	for i:=0;i<int(size);i++ {
		if err := m.Data[i].Serialize(buf); err != nil { return err }
	}
	buf.Flush()
	return nil
}

func (m *StreamDataRequest) Deserialize(buf *bufio.Reader) error {
	var size uint32
	if err := binary.Read(buf, binary.LittleEndian, &size); err != nil { return err }

	m.Data = make([]*StreamData, size)
	for i:=0;i<int(size);i++ {
		m.Data[i] = &StreamData{}
		if err := m.Data[i].Deserialize(buf); err != nil { return err }
	}
	return nil
}

func (m *StreamDataRequest) GetType() uint32 { return STREAM_DATA_REQUEST }

type StreamDataResponse struct {}
func (m *StreamDataResponse) Serialize(*bufio.Writer) error { return nil }
func (m *StreamDataResponse) Deserialize(*bufio.Reader) error { return nil }
func (m *StreamDataResponse) GetType() uint32 { return STREAM_DATA_RESPONSE }

