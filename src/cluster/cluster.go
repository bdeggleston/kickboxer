/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/2/13
 * Time: 6:47 PM
 * To change this template use File | Settings | File Templates.
 */
package cluster

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
)

type Token []byte

const (
	CLUSTER_STARTED = "CLUSTER_STARTED"
	CLUSTER_NORMAL = "CLUSTER_NORMAL"
	CLUSTER_STREAMING = "CLUSTER_STREAMING"
)

const (
	CONSISTENCY_ONE = "CONSISTENCY_ONE"
	CONSISTENCY_QUORUM = "CONSISTENCY_QUORUM"
	CONSISTENCY_ALL = "CONSISTENCY_ALL"
)


// implements sort.Interface
type nodeSorter struct {
	nodes []Node
}

func (ns *nodeSorter) Len() int {
	return len(ns.nodes)
}

// returns true if the item at index i is less than
// the item and index j
func (ns *nodeSorter) Less(i, j int) bool {
	return bytes.Compare(ns.nodes[i].GetToken(), ns.nodes[j].GetToken()) == -1
}

// switches the position of nodes at indices i & j
func (ns *nodeSorter) Swap(i, j int) {
	ns.nodes[i], ns.nodes[j] = ns.nodes[j], ns.nodes[i]
}


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
	tokenRing []Node
	// the state of the ring before the most recent ring mutation
	priorRing []Node

	name string
	token Token
	nodeId NodeId
	peerAddr string
	peerServer *PeerServer
}

func NewCluster(addr string, name string, token Token, nodeId NodeId, replicationFactor uint32) (*Cluster, error) {
	c := &Cluster{}
	c.peerAddr = addr
	c.name = name
	c.token = token
	c.nodeId = nodeId
	c.localNode = NewLocalNode(c.nodeId, c.token, c.name)

	// setup nodemap and initial token ring
	c.nodeMap = make(map[NodeId] Node)
	c.tokenRing = make([]Node, 1, 10)
	c.priorRing = make([]Node, 1, 10)

	c.addNode(c.localNode)
	c.priorRing = c.tokenRing

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

// refreshes the token ring after changes
// this method is not threadsafe
func (c* Cluster) refreshRing() error {
	// create an array from the map
	nodes := make([]Node, len(c.nodeMap))
	idx := 0
	for _, v := range c.nodeMap {
		nodes[idx] = v
		idx++
	}

	// sort by their tokens
	sorter := &nodeSorter{nodes:nodes}
	sort.Sort(sorter)

	// update the ring
	c.priorRing = c.tokenRing
	c.tokenRing = sorter.nodes

	return nil
}

// adds a node to the cluster, if it's not already
// part of the cluster, and starting it if the cluster
// has been started
func (c *Cluster) addNode(node Node) error {
	c.nodeLock.RLock()
	nid := node.GetId()
	_, ok := c.nodeMap[nid]
	c.nodeLock.RUnlock()
	if !ok {
		c.nodeLock.Lock()
		defer c.nodeLock.Unlock()
		c.nodeMap[nid] = node
		if err:= c.refreshRing(); err != nil { return err }
	}
	return nil
}

func (c* Cluster) Start() error {
	return nil
}

func (c* Cluster) Stop() error {
	return nil
}

/************** key routing **************/

// returns the nodes that replicate the given token
// includes the node that owns the token, and it's replicas
func (c *Cluster) GetNodesForToken(t Token) []Node {
	c.nodeLock.RLock()
	defer c.nodeLock.RUnlock()

}

func (c *Cluster) GetNodesForKey(k string) []Node {

}
