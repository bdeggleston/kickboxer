/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/2/13
 * Time: 6:47 PM
 * To change this template use File | Settings | File Templates.
 */
package cluster

import (
	"fmt"
	"sync"
)

type Token []byte

type Cluster struct {
	// nodes addressed to communicate with to
	// discover the rest of the cluster
	seeds []string

	// the number of nodes a key should
	// be replicated to
	replicationFactor uint32

	localNode *LocalNode

	// map of node ids to node objects
	nodeMap map[NodeId] Node
	nodeLock sync.RWMutex

	// nodes ordered by token
	tokenRing [] Node

	name string
	token Token
	nodeId NodeId
	peerAddr string
	peerServer *PeerServer
}

func NewCluster(addr string, name string, token Token, nodeId NodeId) (*Cluster, error) {
	c := &Cluster{}
	c.peerAddr = addr
	c.name = name
	c.token = token
	c.nodeId = nodeId
	c.localNode = NewLocalNode(c.nodeId, c.token, c.name)

	// setup nodemap and initial token ring
	c.nodeMap = make(map[NodeId] Node)
	c.nodeLock.Lock()
	defer c.nodeLock.Unlock()
	c.nodeMap[nodeId] = c.localNode
	c.tokenRing = make([]Node, 1)
	c.tokenRing[0] = c.localNode

	return c, nil
}

// info getters
func (c* Cluster) GetNodeId() NodeId { return c.nodeId }
func (c* Cluster) GetToken() Token { return c.token }
func (c* Cluster) GetName() string { return c.name }
func (c* Cluster) GetPeerAddr() string { return c.peerAddr }

// gets a node by it's node id
func (c *Cluster) getNode(nid NodeId) (Node, error) {
	c.nodeLock.RLock()
	defer c.nodeLock.RUnlock()
	node, ok := c.nodeMap[nid]
	if !ok {
		return nil, fmt.Errorf("No node found by node id: %v", nid)
	}
	return node, nil
}

func (c* Cluster) Start() error {
	return nil
}

func (c* Cluster) Stop() error {
	return nil
}
