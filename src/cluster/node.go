/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/2/13
 * Time: 6:48 PM
 * To change this template use File | Settings | File Templates.
 */
package cluster

import (
	"time"
	"store"
)

type node interface {
	Name() string
	GetToken() Token
	GetId() string

	ExecuteRead(cmd string, key string, args []string)
	ExecuteWrite(cmd string, key string, args []string, timestamp time.Time)
}

type baseNode struct {
	name string
	token Token
	id string
}


type LocalNode struct {
	baseNode
	store store.Store
}

type RemoteNode struct {
	baseNode
}


