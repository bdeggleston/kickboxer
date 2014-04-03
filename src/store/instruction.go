package store

import (
	"bufio"
	"encoding/binary"
	"time"
)

import (
	"serializer"
)

// an instruction to be executed against
// the store. These objects should be
// considered immutable once instantiated
type Instruction struct {
	Cmd string
	Key string
	Args []string
	Timestamp time.Time
}

// creates a new instruction
func NewInstruction(cmd string, key string, args []string, timestamp time.Time) Instruction {
	return Instruction{
		Cmd: cmd,
		Key: key,
		Args: args,
		Timestamp: timestamp,
	}
}

// instruction equality test
func (i *Instruction) Equal(o *Instruction) bool {
	if i.Cmd != o.Cmd { return false }
	if i.Key != o.Key { return false }
	if len(i.Args) != len(o.Args) { return false }
	for n:=0;n<len(i.Args);n++ {
		if i.Args[n] != o.Args[n] { return false}
	}
	if i.Timestamp != o.Timestamp { return false }
	return true
}

func (i *Instruction) Copy() Instruction {
	newInstr := Instruction{
		Cmd: i.Cmd,
		Key: i.Key,
		Args: make([]string, len(i.Args)),
		Timestamp: i.Timestamp,
	}
	copy(newInstr.Args, i.Args)
	return newInstr
}

// returns the expected number of bytes from serialization
func (i *Instruction) NumBytes() (int) {
	numBytes := 0
	numBytes += serializer.NumStringBytes(i.Cmd)
	numBytes += serializer.NumStringBytes(i.Key)

	numBytes += 4  // num args int
	for _, arg := range i.Args {
		numBytes += serializer.NumStringBytes(arg)
	}
	numBytes += serializer.NumTimeBytes()
	return numBytes
}

func (i *Instruction) Serialize(buf *bufio.Writer) error {
	if err := serializer.WriteFieldString(buf, i.Cmd); err != nil { return err }
	if err := serializer.WriteFieldString(buf, i.Key); err != nil { return err }
	numArgs := uint32(len(i.Args))
	if err := binary.Write(buf, binary.LittleEndian, &numArgs); err != nil { return err }
	for _, arg := range i.Args {
		if err := serializer.WriteFieldString(buf, arg); err != nil { return err }
	}
	if err := serializer.WriteTime(buf, i.Timestamp); err != nil { return err }
	return nil
}

func (i *Instruction) Deserialize(buf *bufio.Reader) error {
	if val, err := serializer.ReadFieldString(buf); err != nil { return err } else {
		i.Cmd = val
	}
	if val, err := serializer.ReadFieldString(buf); err != nil { return err } else {
		i.Key = val
	}

	var numArgs uint32
	if err := binary.Read(buf, binary.LittleEndian, &numArgs); err != nil { return err }
	i.Args = make([]string, numArgs)
	for idx := range i.Args {
		if val, err := serializer.ReadFieldString(buf); err != nil { return err } else {
			i.Args[idx] = val
		}
	}
	if val, err := serializer.ReadTime(buf); err != nil { return err } else {
		i.Timestamp = val
	}
	return nil
}
