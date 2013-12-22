package cluster

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const (
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

// TODO: add a checksum to the head of the message
// TODO: gzip message contents

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

func writeFieldString(buf *bufio.Writer, str string) error {
	return writeFieldBytes(buf, []byte(str))
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

func readFieldString(buf *bufio.Reader) (string, error) {
	bytes, err := readFieldBytes(buf)
	return string(bytes), err
}

func writeTime(buf *bufio.Writer, t time.Time) error {
	bytes, err := t.GobEncode()
	if err != nil { return err }
	_, err = buf.Write(bytes)
	return err
}

func readTime(buf *bufio.Reader) (time.Time, error) {
	t := time.Time{}
	bytes := make([]byte, 15)
	if _, err := buf.Read(bytes); err != nil { return t, err }
	if err := t.GobDecode(bytes); err != nil { return t, err }
	return t, nil
}


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
	case STREAM_DATA_REQUEST:
		msg = &StreamDataRequest{}
	case STREAM_DATA_RESPONSE:
		msg = &StreamDataResponse{}
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
