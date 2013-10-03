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

	"code.google.com/p/go-uuid/uuid"
)

type NodeId uuid.UUID

type node interface {
	Name() string
	GetToken() Token
	GetId() NodeId

	// starts up the node
	start()

	// executes a read instruction against the node's store
	ExecuteRead(cmd string, key string, args []string)

	// executes a write instruction against the node's store
	ExecuteWrite(cmd string, key string, args []string, timestamp time.Time)
}

// the baseNode defines all of the properties
// and methods in common among node types
type baseNode struct {
	name string
	token Token
	id NodeId
}


func (n *baseNode) Name() string { return n.name }

func (n *baseNode) GetToken() { return n.token }

func (n *baseNode) GetId() { return n.id }

// LocalNode provides access to the local store
type LocalNode struct {
	baseNode
	store store.Store
}

func NewLocalNode(id NodeId, token Token, name string) (n *LocalNode) {
	//
	n.id = id
	n.token = token
	n.name = name
	return
}

func (n *LocalNode) start() {
	// connect the store
}


// RemoteNode communicates with other nodes in the cluster
type RemoteNode struct {
	baseNode

	// the node's network address
	addr string
}


func NewRemoteNode(addr string) (n *RemoteNode) {
	n.addr = addr
	return
}

func (n *RemoteNode) start() {
	// connect to the node and get it's info
}



