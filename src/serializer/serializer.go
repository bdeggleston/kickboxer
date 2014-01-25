/**

common serialize/deserialize functions

 */
package serializer

import (
	"encoding/binary"
	"fmt"
	"bufio"
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
		return fmt.Errorf("unexpected num bytes written. Expected %v, got %v", size, n)
	}
	return nil
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
		return nil, fmt.Errorf("unexpected num bytes read. Expected %v, got %v", size, n)
	}
	return bytes, nil
}

func WriteFieldString(buf *bufio.Writer, str string) error {
	return WriteFieldBytes(buf, []byte(str))
}

func ReadFieldString(buf *bufio.Reader) (string, error) {
	bytes, err := ReadFieldBytes(buf)
	return string(bytes), err
}

// writes a time value
func WriteTime(buf *bufio.Writer, t time.Time) error {
	b, err := t.GobEncode()
	if err != nil { return err }
	if err := WriteFieldBytes(buf, b); err != nil {
		return err
	}

	return nil
}

// reads a time value
func ReadTime(buf *bufio.Reader) (time.Time, error) {
	t := time.Time{}
	timeBytes, err := ReadFieldBytes(buf)
	if err != nil {
		return t, err
	}
	if err := t.GobDecode(timeBytes); err != nil {
		return t, err
	}
	return t, nil
}
