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
type ConnectionRequest struct {
	// the id of the requesting node
	NodeId NodeId
	// the address of the requesting node
	Addr string
	// the name of the requesting node
	Name string
	// the token of the requesting node
	Token Token
}

func (m *ConnectionRequest) Serialize(buf *bufio.Writer) error {

	// write the number of fields
	numArgs := uint32(3)
	if err := binary.Write(buf, binary.LittleEndian, &numArgs); err != nil { return err }

	// then the fields
	if err := writeFieldBytes(buf, []byte(m.NodeId)); err != nil { return err }
	if err := writeFieldBytes(buf, []byte(m.Addr)); err != nil { return err }
	if err := writeFieldBytes(buf, []byte(m.Name)); err != nil { return err }
	if err := writeFieldBytes(buf, []byte(m.Token[:])); err != nil { return err }

	return nil
}

func (m *ConnectionRequest) Deserialize(buf *bufio.Reader) error {

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
	for i:=0; i<16; i++ { m.NodeId[i] = b[i] }

	b, err = readFieldBytes(buf)
	if err != nil { return nil }
	m.Addr = string(b)

	b, err = readFieldBytes(buf)
	if err != nil { return nil }
	m.Name = string(b)

	b, err = readFieldBytes(buf)
	if err != nil { return nil }
	for i:=0; i<16; i++ { m.Token[i] = b[i] }

	return nil
}

