package cluster

import (
	"crypto/md5"
)

import (
	"partitioner"
)

type Partitioner interface {

	GetToken(key string) partitioner.Token
}

// partitions on a keys md5 hash
type MD5Partitioner struct {

}

func (p MD5Partitioner) GetToken(key string) partitioner.Token {
	h := md5.New()
	h.Write([]byte(key))
	return partitioner.Token(h.Sum(nil))
}

func NewMD5Partitioner() *MD5Partitioner {
	return &MD5Partitioner{}
}


