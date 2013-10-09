/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/9/13
 * Time: 2:13 PM
 * To change this template use File | Settings | File Templates.
 */
package cluster

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

// returns the number passed into the key,
// keys can only be stringified ints
type literalPartitioner struct {

}

func (p literalPartitioner) GetToken(key string) Token {
	val, err := strconv.Atoi(key)
	if err != nil {
		panic(fmt.Sprintf("The given key does not convert to an integer: %v", key))
	}
	if val < 0 {
		panic(fmt.Sprintf("The given key is a negative number: %v", key))
	}
	uval := uint64(val)
	b := make([]byte, 8)

	if err := binary.Write(b, binary.LittleEndian, &uval); err != nil {
		panic(fmt.Sprintf("There was an error encoding the token: %v", err))
	}
	return Token(b)
}

