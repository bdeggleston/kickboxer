/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/9/13
 * Time: 2:00 PM
 * To change this template use File | Settings | File Templates.
 */
package cluster

import (
	"crypto/md5"
)

type Partitioner interface {

	GetToken(key string) Token
}

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


