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
)

type Token []byte

type ClusterStatus string

const (
	CLUSTER_INITIALIZING 	= ClusterStatus("")
	CLUSTER_NORMAL 			= ClusterStatus("NORMAL")
	CLUSTER_STREAMING 		= ClusterStatus("STREAMING")
)

type ConsistencyLevel string

const (
	CONSISTENCY_ONE 	= ConsistencyLevel("ONE")
	CONSISTENCY_QUORUM 	= ConsistencyLevel("QUORUM")
	CONSISTENCY_ALL 	= ConsistencyLevel("ALL")
)


// TODO: move to cluster.Ring.go
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

	ring *Ring

	name string
	token Token
	nodeId NodeId
	peerAddr string
	peerServer *PeerServer
	partitioner Partitioner

	status ClusterStatus
}

func NewCluster(
	// the address the peer server will be listening on
	addr string,
	// the name of this local node
	name string,
	// the token of this local node
	token Token,
	// the id of this local node
	nodeId NodeId,
	// the replication factor of the cluster
	replicationFactor uint32,
	// the partitioner used by the cluster
	partitioner Partitioner,

) (*Cluster, error) {
	//
	c := &Cluster{}
	c.status = CLUSTER_INITIALIZING
	c.peerAddr = addr
	c.name = name
	c.token = token
	c.nodeId = nodeId
	c.localNode = NewLocalNode(c.nodeId, c.token, c.name)

	c.peerServer = NewPeerServer(c, c.peerAddr)

	if replicationFactor < 1 {
		return nil, fmt.Errorf("Invalid replication factor: %v", replicationFactor)
	}
	c.replicationFactor = replicationFactor
	if partitioner == nil {
		return nil, fmt.Errorf("partitioner cannot be nil")
	}
	c.partitioner = partitioner

	c.ring = NewRing()
	c.ring.AddNode(c.localNode)

	return c, nil
}

// info getters
func (c* Cluster) GetNodeId() NodeId { return c.nodeId }
func (c* Cluster) GetToken() Token { return c.token }
func (c* Cluster) GetName() string { return c.name }
func (c* Cluster) GetPeerAddr() string { return c.peerAddr }

// adds a node to the cluster, if it's not already
// part of the cluster, and starting it if the cluster
// has been started
func (c *Cluster) addNode(node Node) error {
	// add to ring, and start if it hasn't been seen before
	if err := c.ring.AddNode(node); err == nil {
		if c.status != CLUSTER_INITIALIZING {
			if err := node.Start(); err != nil { return err }
		}
	}
	return nil
}

// returns data on peer nodes
func (c *Cluster) getPeerData() *PeerData {
	return nil
}

func (c* Cluster) Start() error {
	// start listening for connections
	if err := c.peerServer.Start(); err != nil {
		return err
	}

	//startup the nodes
	for _, node := range c.ring.AllNodes() {
		if !node.IsStarted() {
			if err:= node.Start(); err != nil {
				return err
			}
		}
	}

	// check for additional nodes
//	if err := c.discoverPeers(); err != nil {
//		return err
//	}

	c.status = CLUSTER_NORMAL

	return nil
}

func (c* Cluster) Stop() error {
	c.peerServer.Stop()
	for _, node := range c.ring.AllNodes() {
		node.Stop()
	}
	return nil
}

/************** key routing **************/

// gets the token of the given key and returns the nodes
// that it maps to
func (c *Cluster) GetNodesForKey(k string) []Node {
	token := c.partitioner.GetToken(k)
	return c.ring.GetNodesForToken(token, c.replicationFactor)
}

