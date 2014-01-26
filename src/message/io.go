package message

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

// function that returns an empty message
type messageConstructor func() Message

var constructors = make(map[uint32] messageConstructor)

func RegisterMessage(mtype uint32, constructor messageConstructor) {
	constructors[mtype] = constructor
}

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
func ReadMessage(buf io.Reader) (Message, error) {
	reader := bufio.NewReader(buf)
	var mtype uint32
	if err := binary.Read(reader, binary.LittleEndian, &mtype); err != nil { return nil, err }

	// iterate over the message types
	constructor, ok := constructors[mtype]
	if !ok { return nil, fmt.Errorf("Unknown message type: %v", mtype) }
	msg := constructor()
	if err := msg.Deserialize(reader); err != nil { return nil, err }
	return msg, nil
}

