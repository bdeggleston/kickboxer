package cluster

import (
	"bytes"
	"fmt"
)

import (
	"store"
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
	// the local store
	store store.Store

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
	dcId DatacenterId
	peerAddr string
	peerServer *PeerServer
	partitioner Partitioner

	status ClusterStatus
}

func NewCluster(
	// the local store
	store store.Store,
	// the address the peer server will be listening on
	addr string,
	// the name of this local node
	name string,
	// the token of this local node
	token Token,
	// the id of this local node
	nodeId NodeId,
	// the name of the datacenter this node belongs to
	dcId DatacenterId,
	// the replication factor of the cluster
	replicationFactor uint32,
	// the partitioner used by the cluster
	partitioner Partitioner,
	// list of seed node addresses
	seeds []string,

) (*Cluster, error) {
	//
	c := &Cluster{}
	c.store = store
	c.status = CLUSTER_INITIALIZING
	c.peerAddr = addr
	c.name = name
	c.token = token
	c.nodeId = nodeId
	c.dcId = dcId
	c.localNode = NewLocalNode(c.nodeId, c.dcId, c.token, c.name, c.store)

	c.peerServer = NewPeerServer(c, c.peerAddr)

	if replicationFactor < 1 {
		return nil, fmt.Errorf("Invalid replication factor: %v", replicationFactor)
	}
	c.replicationFactor = replicationFactor
	if partitioner == nil {
		return nil, fmt.Errorf("partitioner cannot be nil")
	}
	c.partitioner = partitioner

	if seeds == nil {
		c.seeds = []string{}
	} else {
		c.seeds = seeds
	}

	c.ring = NewRing()
	c.ring.AddNode(c.localNode)

	return c, nil
}

// info getters
func (c* Cluster) GetNodeId() NodeId { return c.nodeId }
func (c* Cluster) GetDatacenterId() DatacenterId { return c.dcId }
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
func (c *Cluster) getPeerData() []*PeerData {
	nodes := c.ring.AllNodes()
	peers := make([]*PeerData, 0, len(nodes) - 1)
	for _, node := range nodes {
		if node.GetId() != c.GetNodeId() {
			pd := &PeerData{
				NodeId:node.GetId(),
				Addr:node.GetAddr(),
				Name:node.Name(),
				Token:node.GetToken(),
			}
			peers = append(peers, pd)
		}
	}
	return peers
}

// talks to the seed addresses and any additional
// remote nodes we're already aware of to discover
// new node
func (c* Cluster) discoverPeers() error {

	// checks the existing nodes for the given address
	addrIsKnown := func(addr string) *RemoteNode {
		for _, v := range c.ring.AllNodes() {
			if node, ok := v.(*RemoteNode); ok {
				if node.addr == addr {
					return node
				}
			}
		}
		return nil
	}

	// add seed nodes
	for _, addr := range c.seeds {
		if node := addrIsKnown(addr); node == nil {
			node := NewRemoteNode(addr, c)
			// start the node to get it's info
			if err := node.Start(); err != nil {
				fmt.Println(err)
				continue
			}
			c.ring.AddNode(node)
		}
	}

	// get peer info from existing nodes
	getRemoteNodes := func() []*RemoteNode {
		peers := make([]*RemoteNode, 0)
		for _, v := range c.ring.AllNodes() {
			if node, ok := v.(*RemoteNode); ok {
				peers = append(peers, node)
			}
		}
		return peers
	}
	peers := getRemoteNodes()
	request := &DiscoverPeersRequest{NodeId:c.GetNodeId()}
	for _, node := range peers {
		// don't add yourself
		if node.GetId() == c.GetNodeId() {
			continue
		}
		response, _, err := node.sendMessage(request)
		if err != nil { return err }
		peerMessage, ok := response.(*DiscoverPeerResponse)
		if !ok {
			return fmt.Errorf("Unexpected message type. Expected *DiscoverPeerResponse, got %T", response)
		}
		for _, peer := range peerMessage.Peers {
			n := NewRemoteNodeInfo(
				peer.NodeId,
				peer.DCId,
				peer.Token,
				peer.Name,
				peer.Addr,
				c,
			)
			if err := c.ring.AddNode(n); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c* Cluster) Start() error {
	// check for existing nodes
	firstStartup := len(c.ring.AllNodes()) == 0

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
	if err := c.discoverPeers(); err != nil {
		return err
	}

	if firstStartup {
		// join the cluster, and stream from from the left
		// neighbor
		if err := c.JoinCluster(); err != nil {
			return err
		}
	} else {
		c.status = CLUSTER_NORMAL
	}

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

/************** streaming **************/

// initiates streaming tokens from the given node
func (c *Cluster) streamFromNode(n Node) error {
	node := n.(*RemoteNode)
	msg := &StreamRequest{}
	_, mtype, err := node.sendMessage(msg)
	if err != nil { return err }
	if mtype != STREAM_RESPONSE {
		return fmt.Errorf("Expected STREAM_RESPONSE, got: %v", mtype)
	}
	c.status = CLUSTER_STREAMING
	return nil
}

// streams keys that are owned/replicated
// by the given node to it
func (c *Cluster) streamToNode(n Node) error {
	//
	node := n.(*RemoteNode)

	// determines if the given key is replicated by
	// the destination node
	replicates:= func(key string) bool {
		nodes := c.GetNodesForKey(key)
		for _, rnode := range nodes {
			if rnode.GetId() == node.GetId() {
				return true
			}
		}
		return false
	}

	// iterate over the keys and send replicated k/v
	keys := c.store.GetKeys()
	for _, key := range keys {
		if replicates(key) {
			val, err := c.store.GetRawKey(key)
			if err != nil { return err }
			valBytes, err := c.store.SerializeValue(val)
			if err != nil { return err }
			sd := &StreamData{Key:key, Data:valBytes}
			msg := &StreamDataRequest{Data:[]*StreamData{sd}}
			response , _, err := node.sendMessage(msg)
			if err != nil { return err }
			if response.GetType() != STREAM_DATA_RESPONSE {
				return fmt.Errorf("Expected StreamDataResponse, got %T", response)
			}
		}
	}

	// notify remote node that streaming is completed
	response, _, err := node.sendMessage(&StreamCompleteRequest{})
	if err != nil { return err }
	if response.GetType() != STREAM_COMPLETE_RESPONSE {
		return fmt.Errorf("Expected StreamCompleteRequest, got %T", response)
	}
	return nil
}

// receives streaming requests from other nodes
//
// if the key exists on this node, the incoming value
// should be compared against the local value, and if
// there are differences, a reconciliation of the key
// should be performed
func receiveStreamedData([]*StreamData) error {
	return nil
}

/************** node changes **************/

// called when a node is first added to the cluster
//
// When changing the token ring from this:
// N0      N1      N2      N3      N4      N5      N6      N7      N8      N9
// [00    ][10    ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]
//
// to this:
// N0  N10 N1      N2      N3      N4      N5      N6      N7      N8      N9
// [00][05][10    ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]
// |--|->
//
// N10 should stream data from the node to it's left, since it's taking control
// of a portion of it's previous token space
func (c *Cluster) JoinCluster() error {
	ring := c.ring.AllNodes()
	var idx int
	for i, node := range ring {
		if node.GetId() == c.GetNodeId() {
			idx = i
			break
		}
	}

	// check that the node at the idx matches this cluster's id
	if ring[idx].GetId() != c.GetNodeId() {
		panic("node at index is not the local node")
	}

	stream_from := ring[(idx - 1) % len(ring)]
	c.streamFromNode(stream_from)
	return nil
}
