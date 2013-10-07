/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/2/13
 * Time: 6:48 PM
 * To change this template use File | Settings | File Templates.
 */
package cluster

import (
	"fmt"
	"time"
	"store"

	"code.google.com/p/go-uuid/uuid"
)

type NodeId string

type NodeError struct {
	reason string
}

func NewNodeError(reason string) *NodeError {
	return &NodeError{reason:reason}
}

func (e *NodeError) Error() string {
	return e.reason
}

func NewNodeId() NodeId {
	return NodeId(uuid.NewRandom().String())
}

type Node interface {
	Name() string
	GetToken() Token
	GetId() NodeId

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

func (n *baseNode) GetToken() Token { return n.token }

func (n *baseNode) GetId() NodeId { return n.id }

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

	pool ConnectionPool
	cluster *Cluster
}


func NewRemoteNode(addr string, cluster *Cluster) (n *RemoteNode) {
	n.addr = addr
	n.pool = *NewConnectionPool(n.addr, 10, 10000)
	n.cluster = cluster
	return
}

func (n *RemoteNode) start() {
	// connect to the node and get it's info
}

// returns a connection with a completed handshake
func (n *RemoteNode) getConnection() (*Connection, error) {

	conn, err := n.pool.Get()
	if err != nil { return nil, err }

	if !conn.HandshakeCompleted() {
		msg := &ConnectionRequest{PeerData{
			NodeId:n.cluster.GetNodeId(),
			Addr:n.cluster.GetPeerAddr(),
			Name:n.cluster.GetName(),
			Token:n.cluster.GetToken(),
		}}
		if err := WriteMessage(conn, msg); err != nil { return nil, err }
		response, mtype, err := ReadMessage(conn)
		if err != nil { return nil, err }
		if mtype != CONNECTION_ACCEPTED_RESPONSE {
			return nil, fmt.Errorf("Unexpected response type, expected *ConnectionAcceptedResponse, got %T", response)
		}
		conn.SetHandshakeCompleted()
	}
	return conn, nil
}

func (n *RemoteNode) sendMessage(m Message) (Message, uint32, error) {

	// get connection
	conn, err := n.getConnection()
	if  err != nil { return nil, 0, err }


	// send the message
	if err := WriteMessage(conn, m); err != nil {
		conn.Close()
		return nil, 0, err
	}

	// receive the message
	response, messageType, err := ReadMessage(conn)
	if err != nil { return nil, 0, err }

	return response, messageType, nil
}


