package partitioner

import (
	"launchpad.net/gocheck"
)

type MD5PartitionerTest struct {}

var _ = gocheck.Suite(&MD5PartitionerTest{})

func (t *MD5PartitionerTest) TestHashes(c *gocheck.C) {
	p := NewMD5Partitioner()
	c.Check(
		p.GetToken("1234"),
		gocheck.DeepEquals,
		Token([]byte{129, 220, 155, 219, 82, 208, 77, 194, 0, 54, 219, 216, 49, 62, 208, 85}),
	)
	c.Check(
		p.GetToken("blake"),
		gocheck.DeepEquals,
		Token([]byte{58, 164, 158, 198, 191, 201, 16, 100, 127, 161, 197, 160, 19, 228, 142, 239}),
	)
	c.Check(
		p.GetToken("abc"),
		gocheck.DeepEquals,
		Token([]byte{144, 1, 80, 152, 60, 210, 79, 176, 214, 150, 63, 125, 40, 225, 127, 114}),
	)
}
