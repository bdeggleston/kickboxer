package partitioner

import (
	"crypto/md5"
)

// partitions on a keys md5 hash
type MD5Partitioner struct {

}

func (p MD5Partitioner) GetToken(key string) Token {
	h := md5.New()
	h.Write([]byte(key))
	return Token(h.Sum(nil))
}

func NewMD5Partitioner() *MD5Partitioner {
	return &MD5Partitioner{}
}
