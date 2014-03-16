package types

import (
	"bytes"
)

import (
	"launchpad.net/gocheck"
	"code.google.com/p/go-uuid/uuid"
)

type UUIDTest struct {}

var _ = gocheck.Suite(&UUIDTest{})

func (s *UUIDTest) TestEncoding(c *gocheck.C) {
	uuidBytes := []byte(uuid.NewUUID())
	c.Logf("%v", uuidBytes)

	u := UUID{}
	err := (&u).UnmarshalBinary(uuidBytes)
	c.Assert(err, gocheck.IsNil)

	marshalledBytes, err := (&u).MarshalBinary()
	c.Assert(err, gocheck.IsNil)
	c.Logf("%v", marshalledBytes)
	c.Logf("%v", u)

	c.Assert(bytes.Compare(uuidBytes, marshalledBytes), gocheck.Equals, 0)
}

func (s *UUIDTest) TestTime(c *gocheck.C) {
	extU := uuid.NewUUID()

	intU := UUID{}
	err := (&intU).UnmarshalBinary([]byte(extU))
	c.Assert(err, gocheck.IsNil)

	extTime, _ := extU.Time()
	intTime := intU.Time()

	c.Logf("%v", intTime)
	c.Assert(int64(extTime), gocheck.Equals, intTime)
	c.Assert(intTime > 0, gocheck.Equals, true)
}

