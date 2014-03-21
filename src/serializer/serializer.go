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

// reads the specified number of bytes out
// of the reader, performing multiple reads
// if neccesary
func ReadBytes(buf *bufio.Reader, size int) ([]byte, error) {
	numRead := 0
	target := make([]byte, size)
	for numRead < size {
		n, err := buf.Read(target[numRead:])
		if err != nil {
			return nil, err
		}
		numRead += n
	}
	return target, nil
}

// read field bytes
func ReadFieldBytes(buf *bufio.Reader) ([]byte, error) {
	var size uint32
	if err := binary.Read(buf, binary.LittleEndian, &size); err != nil {
		return nil, err
	}

	bytesRead, err := ReadBytes(buf, int(size))
	if err != nil {
		return nil, err
	}

	if n := len(bytesRead); n != int(size) {
		return nil, fmt.Errorf("unexpected num bytes read. Expected %v, got %v", size, n)
	}
	return bytesRead, nil
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

func NumStringBytes(s string) int {
	return 4 + len(s)
}

func NumSliceBytes(b []byte) int {
	return 4 + len(b)
}

func NumTimeBytes() int {
	return 15
}

