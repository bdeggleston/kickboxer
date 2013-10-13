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

type NodeStatus string

const (
	NODE_INITIALIZING 	= NodeStatus("")
	NODE_UP 			= NodeStatus("UP")
	NODE_DOWN 			= NodeStatus("DOWN")
)

type NodeId string

func NewNodeId() NodeId {
	return NodeId(uuid.NewRandom().String())
}

func (nid NodeId) IsNil() bool {
	return nid == NodeId("")
}

type NodeError struct {
	reason string
}

func NewNodeError(reason string) *NodeError {
	return &NodeError{reason:reason}
}

func (e *NodeError) Error() string {
	return e.reason
}

type Node interface {
	Name() string
	GetToken() Token
	GetId() NodeId
	GetStatus() NodeStatus

	Start() error
	Stop() error
	IsStarted() bool

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
	status NodeStatus
}

func (n *baseNode) Name() string { return n.name }

func (n *baseNode) GetToken() Token { return n.token }

func (n *baseNode) GetId() NodeId { return n.id }

func (n *baseNode) GetStatus() NodeStatus { return n.status }

// LocalNode provides access to the local store
type LocalNode struct {
	baseNode
	store store.Store
	isStarted bool
}

func NewLocalNode(id NodeId, token Token, name string) (*LocalNode) {
	//
	n := &LocalNode{}
	n.id = id
	n.token = token
	n.name = name
	n.status = NODE_UP
	return n
}

func (n *LocalNode) Start() error {
	// connect the store
	n.isStarted = true
	return nil
}

func (n *LocalNode) Stop() error {
	n.isStarted = false
	return nil
}

func (n *LocalNode) IsStarted() bool {
	return n.isStarted
}

// executes a read instruction against the node's store
func (n *LocalNode) ExecuteRead(cmd string, key string, args []string) {
	_ = cmd
	_ = key
	_ = args

}

// executes a write instruction against the node's store
func (n *LocalNode) ExecuteWrite(cmd string, key string, args []string, timestamp time.Time) {
	_ = cmd
	_ = key
	_ = args
	_ = timestamp

}

// RemoteNode communicates with other nodes in the cluster
type RemoteNode struct {
	baseNode

	// the node's network address
	addr string

	pool ConnectionPool
	cluster *Cluster

	isStarted bool
}


// creates a new remote node from only an address
func NewRemoteNode(addr string, cluster *Cluster) (*RemoteNode) {
	n := &RemoteNode{}
	n.addr = addr
	n.pool = *NewConnectionPool(n.addr, 10, 10000)
	n.cluster = cluster
	return n
}

// creates a new remote node from info provided from the node
func NewRemoteNodeInfo(id NodeId, token Token, name string, addr string, cluster *Cluster) (n *RemoteNode) {
	n = NewRemoteNode(addr, cluster)
	n.id = id
	n.token = token
	n.name = name
	return n
}

func (n *RemoteNode) Start() error {
	// connect to the node and get it's info
	conn, err := n.getConnection()
	if err != nil {
		return err
	}
	n.pool.Put(conn)
	n.status = NODE_UP
	n.isStarted = true
	return nil
}

func (n *RemoteNode) Stop() error {
	// connect to the node and get it's info
	n.isStarted = false
	return nil
}

func (n *RemoteNode) IsStarted() bool {
	return n.isStarted
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
		if err := WriteMessage(conn, msg); err != nil {
			n.status = NODE_DOWN
			return nil, err
		}
		response, mtype, err := ReadMessage(conn)
		if err != nil {
			n.status = NODE_DOWN
			return nil, err
		}
		if mtype != CONNECTION_ACCEPTED_RESPONSE {
			return nil, fmt.Errorf("Unexpected response type, expected *ConnectionAcceptedResponse, got %T", response)
		}

		// copy the response info if we're still initializing
		if n.status == NODE_INITIALIZING {
			accept := response.(*ConnectionAcceptedResponse)
			n.id = accept.NodeId
			n.name = accept.Name
			n.token = accept.Token
		}

		conn.SetHandshakeCompleted()
	}
	return conn, nil
}

func (n *RemoteNode) sendMessage(m Message) (Message, uint32, error) {

	// get connection
	conn, err := n.getConnection()
	if  err != nil {
		n.status = NODE_DOWN
		return nil, 0, err
	}


	// send the message
	if err := WriteMessage(conn, m); err != nil {
		conn.Close()
		n.status = NODE_DOWN
		return nil, 0, err
	}

	// receive the message
	response, messageType, err := ReadMessage(conn)
	if err != nil {
		n.status = NODE_DOWN
		return nil, 0, err
	}

	n.status = NODE_UP
	return response, messageType, nil
}

// executes a read instruction against the node's store
func (n *RemoteNode) ExecuteRead(cmd string, key string, args []string) {
	_ = cmd
	_ = key
	_ = args

}

// executes a write instruction against the node's store
func (n *RemoteNode) ExecuteWrite(cmd string, key string, args []string, timestamp time.Time) {
	_ = cmd
	_ = key
	_ = args
	_ = timestamp

}

