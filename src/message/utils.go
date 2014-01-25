package message

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"time"
)

// writes the field length, then the field to the writer
func WriteFieldBytes(buf *bufio.Writer, bytes []byte) error {
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

func WriteFieldString(buf *bufio.Writer, str string) error {
	return WriteFieldBytes(buf, []byte(str))
}

// read field bytes
func ReadFieldBytes(buf *bufio.Reader) ([]byte, error) {
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

func ReadFieldString(buf *bufio.Reader) (string, error) {
	bytes, err := ReadFieldBytes(buf)
	return string(bytes), err
}

func WriteTime(buf *bufio.Writer, t time.Time) error {
	bytes, err := t.GobEncode()
	if err != nil { return err }
	_, err = buf.Write(bytes)
	return err
}

func ReadTime(buf *bufio.Reader) (time.Time, error) {
	t := time.Time{}
	bytes := make([]byte, 15)
	if _, err := buf.Read(bytes); err != nil { return t, err }
	if err := t.GobDecode(bytes); err != nil { return t, err }
	return t, nil
}
