package types

import (
	"bytes"
)


import (
	"launchpad.net/gocheck"
)

type B16Test struct {}

var _ = gocheck.Suite(&B16Test{})

func (t *B16Test) TestEncoding(c *gocheck.C) {
	uuidBytes := []byte{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15}

	b := B16{}
	err := (&b).UnmarshalBinary(uuidBytes)
	c.Assert(err, gocheck.IsNil)

	marshalledBytes, err := (&b).MarshalBinary()
	c.Assert(err, gocheck.IsNil)

	c.Assert(bytes.Compare(uuidBytes, marshalledBytes), gocheck.Equals, 0)
}

func (t *B16Test) TestIsZero(c *gocheck.C) {
	c.Check((&B16{0, 0}).IsZero(), gocheck.Equals, true)
	c.Check((&B16{1, 0}).IsZero(), gocheck.Equals, false)
	c.Check((&B16{0, 1}).IsZero(), gocheck.Equals, false)
	c.Check((&B16{1, 1}).IsZero(), gocheck.Equals, false)
}
