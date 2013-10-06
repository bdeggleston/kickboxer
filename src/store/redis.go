/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/5/13
 * Time: 7:16 PM
 * To change this template use File | Settings | File Templates.
 */
package store

import (
//	"bufio"
//	"reflect"
	"time"
)

type simpleValue struct {
	data string
	time time.Time
}


type Redis struct {

	data map[string] simpleValue

}

