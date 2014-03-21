package store

import (
	"bufio"
	"bytes"
	"time"
)

import (
	"launchpad.net/gocheck"
)

type InstructionTest struct { }

var _ = gocheck.Suite(&InstructionTest{})

func (t *InstructionTest) TestSerialization(c *gocheck.C) {
	var err error
	src := NewInstruction("SET", "ABC", []string{"x", "y", "z"}, time.Now())
	buf := &bytes.Buffer{}

	writer := bufio.NewWriter(buf)
	err = src.Serialize(writer)
	c.Assert(err, gocheck.IsNil)
	err = writer.Flush()
	c.Assert(err, gocheck.IsNil)

	dst := &Instruction{}
	reader := bufio.NewReader(buf)
	err = dst.Deserialize(reader)
	c.Assert(err, gocheck.IsNil)
	c.Check(dst, gocheck.DeepEquals, src)
}

func (t *InstructionTest) TestNumBytesCalculation(c *gocheck.C) {
	var err error
	src := NewInstruction("SET", "ABC", []string{"x", "y", "z"}, time.Now())
	buf := &bytes.Buffer{}

	writer := bufio.NewWriter(buf)
	err = src.Serialize(writer)
	c.Assert(err, gocheck.IsNil)
	err = writer.Flush()
	c.Assert(err, gocheck.IsNil)

	c.Check(len(buf.Bytes()), gocheck.Equals, src.GetNumBytes())

}
