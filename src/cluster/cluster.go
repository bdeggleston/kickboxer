/*
Handles internode organization and communication
 */
package cluster

import (
	"fmt"
	"time"
)

import (
	logging "github.com/op/go-logging"
)

import (
	"node"
	"store"
)

var logger *logging.Logger

func init() {
	logger = logging.MustGetLogger("monitor")
}

type Token []byte

type ClusterStatus string

const (
	CLUSTER_INITIALIZING 	= ClusterStatus("")
	CLUSTER_NORMAL 			= ClusterStatus("NORMAL")
	CLUSTER_STREAMING 		= ClusterStatus("STREAMING")
)

type ConsistencyLevel string

const (
	CONSISTENCY_ONE 			= ConsistencyLevel("ONE")
	CONSISTENCY_QUORUM			= ConsistencyLevel("QUORUM")
	CONSISTENCY_QUORUM_LOCAL 	= ConsistencyLevel("QUORUM_LOCAL")
	CONSISTENCY_ALL 			= ConsistencyLevel("ALL")
	CONSISTENCY_ALL_LOCAL		= ConsistencyLevel("ALL_LOCAL")
	CONSISTENCY_CONSENSUS		= ConsistencyLevel("CONSENSUS")
	CONSISTENCY_CONSENSUS_LOCAL	= ConsistencyLevel("CONSENSUS_LOCAL")
)

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
	dcContainer *DatacenterContainer

	name string
	token Token
	nodeId node.NodeId
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
	nodeId node.NodeId,
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
	c.dcContainer = NewDatacenterContainer()

	return c, nil
}

// info getters
func (c* Cluster) GetNodeId() node.NodeId { return c.nodeId }
func (c* Cluster) GetDatacenterId() DatacenterId { return c.dcId }
func (c* Cluster) GetToken() Token { return c.token }
func (c* Cluster) GetName() string { return c.name }
func (c* Cluster) GetPeerAddr() string { return c.peerAddr }

// adds a node to the cluster, if it's not already
// part of the cluster, and starting it if the cluster
// has been started
func (c *Cluster) addNode(node ClusterNode) error {
	// add to ring, and start if it hasn't been seen before
	var err error
	if node.GetDatacenterId() == c.GetDatacenterId() {
		err = c.ring.AddNode(node)
	} else {
		err = c.dcContainer.AddNode(node)
	}
	if err != nil { return err }
	if c.status != CLUSTER_INITIALIZING {
		if err := node.Start(); err != nil { return err }
	}
	return nil
}

// returns data on peer nodes
func (c *Cluster) getPeerData() []*PeerData {
	localNodes := c.ring.AllNodes()
	extNodes := c.dcContainer.AllNodes()
	peers := make([]*PeerData, 0, len(localNodes) + len(extNodes) - 1)
	node2PeerData := func(node ClusterNode) *PeerData {
		return &PeerData{
			NodeId:node.GetId(),
			DCId:node.GetDatacenterId(),
			Addr:node.GetAddr(),
			Name:node.Name(),
			Token:node.GetToken(),
		}
	}

	for _, node := range localNodes {
		if node.GetId() != c.GetNodeId() {
			peers = append(peers, node2PeerData(node))
		}
	}
	for _, node := range extNodes {
		peers = append(peers, node2PeerData(node))
	}
	return peers
}

// talks to the seed addresses and any additional
// remote nodes we're already aware of to discover
// new node
func (c* Cluster) discoverPeers() error {

	// checks the existing nodes for the given address
	addrIsKnown := func(addr string) *RemoteNode {
		nodes := c.ring.AllNodes()
		nodes = append(nodes, c.dcContainer.AllNodes()...)
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
			c.addNode(node)
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
		response, _, err := node.SendMessage(request)
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
			if err := c.addNode(n); err != nil {
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
func (c *Cluster) GetLocalNodesForKey(k string) []ClusterNode {
	token := c.partitioner.GetToken(k)
	return c.ring.GetNodesForToken(token, c.replicationFactor)
}

// returns a map of DC id -> nodes for the give key
func (c *Cluster) GetNodesForKey(k string) map[DatacenterId][]ClusterNode {
	token := c.partitioner.GetToken(k)
	nm := c.dcContainer.GetNodesForToken(token, c.replicationFactor)
	nm[c.GetDatacenterId()] = c.ring.GetNodesForToken(token, c.replicationFactor)
	return nm
}

/************** streaming **************/

// initiates streaming tokens from the given node
func (c *Cluster) streamFromNode(n ClusterNode) error {
	node := n.(*RemoteNode)
	msg := &StreamRequest{}
	_, mtype, err := node.SendMessage(msg)
	if err != nil { return err }
	if mtype != STREAM_RESPONSE {
		return fmt.Errorf("Expected STREAM_RESPONSE, got: %v", mtype)
	}
	c.status = CLUSTER_STREAMING
	return nil
}

// streams keys that are owned/replicated
// by the given node to it
func (c *Cluster) streamToNode(n ClusterNode) error {
	//
	node := n.(*RemoteNode)

	// determines if the given key is replicated by
	// the destination node
	replicates:= func(key string) bool {
		nodes := c.GetLocalNodesForKey(key)
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
			response , err := node.SendMessage(msg)
			if err != nil { return err }
			if response.GetType() != STREAM_DATA_RESPONSE {
				return fmt.Errorf("Expected StreamDataResponse, got %T", response)
			}
		}
	}

	// notify remote node that streaming is completed
	response, err := node.SendMessage(&StreamCompleteRequest{})
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

// Changes the given node's token and initiates streaming from new replica nodes
//
// When changing the token ring from this:
// N0      N1      N2      N3      N4      N5      N6      N7      N8      N9
// [00    ][10    ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]
// --> --> --> --> --> --> --> --> --> --> -->|
// to this:
// N0              N2      N3      N4      N5      N6  N1* N7      N8      N9
// [00            ][20    ][30    ][40    ][50    ][60][65][70    ][80    ][90    ]
// <-------|------|                        |--|->
// |------|----------->
//
// N0 should now control N1's old tokens, and N1 should control half of N6's tokens
//
// After the token has been changed, each node should check if the node to it's left
// has changed. If it has, it should stream data from the left. If the node to the right
// has changed, then it should stream data from the right
//
// There is also
//
// If a node starts streaming in data as soon as it knows it's token space changes, there
// will be a race condition that may prevent the correct data being streamed to the node
// if the node doing the streaming is not aware of the token when it receives the request.
func (c *Cluster) MoveNode(token Token) error {
	panic("not implemented")
	return nil
}

// removes the given node from the token ring
//
// there are 2 scenarios to deal with in regards to streaming data in:
//
// * if the removed node is still reachable, it should stream it's data
// to it's previous left node
//
// removing N1
// N0      N1      N2      N3      N4      N5      N6      N7      N8      N9
// [0     ][10    ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]
// |xxxxxx|
// to this:
// N0              N2      N3      N4      N5      N6      N7      N8      N9
// [0             ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]
// ^^^^^^
// [10xxxx]
//
//
// * if the removed node is no longer reachable, the removed node's left node
// should stream the removed node's right node
//
// removing N1
// N0      N1      N2      N3      N4      N5      N6      N7      N8      N9
// [0     ][10    ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]
// |xxxxxx|
// to this:
// N0              N2      N3      N4      N5      N6      N7      N8      N9
// [0             ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]
// <------|------|
//
// N0 should now control N1's old tokens and  N0 should stream data from N2
//
// After the node is removed from the ring, each node should check if the node to
// it's right has changed, if it has, it should stream data from it. If the node
// to it's left has changed, it should not stream data from that node, since it
// was already replicating the token space that the new node was responsible for
func (c *Cluster) RemoveNode() error {
	panic("not implemented")
	return nil
}

/************** queries **************/

// struct used to communicate query
// results over channels
type queryResponse struct {
	nid node.NodeId
	val store.Value
	err error
}

// returns the total number of nodes in a node map
func numMappedNodes(replicaMap map[DatacenterId][]ClusterNode) int {
	num := 0
	for _, nodes := range replicaMap {
		num += len(nodes)
	}
	return num
}

// returns true if the consistency level only
// requires talking to local nodes
func readLocalOnly(cl ConsistencyLevel) bool {
	switch cl {
	case CONSISTENCY_ONE:
		return true
	case CONSISTENCY_QUORUM_LOCAL:
		return true
	case CONSISTENCY_ALL_LOCAL:
		return true
	case CONSISTENCY_CONSENSUS_LOCAL:
		return true
	default:
		return false
	}
	return false
}

type baseNodeError string
func (ne baseNodeError) Error() string { return string(ne) }

// error returned on node timeout
type queryError baseNodeError
func (ne queryError) Error() string { return string(ne) }
type nodeTimeoutError queryError
func (ne nodeTimeoutError) Error() string { return string(ne) }

// reconciles values and issues repair statements to other nodes
func (c *Cluster) reconcileRead(
	key string,
	nodeMap map[node.NodeId]ClusterNode,
	rchan chan queryResponse,
	timeout time.Duration,
) {
	numNodes := len(nodeMap)
	values := make(map[string]store.Value, numNodes)
	var response queryResponse

	numReceived := 0
	timeoutEvent := time.After(timeout * time.Millisecond)
	receive:
		for numReceived < numNodes {
			select {
			case response = <-rchan:
				// do something
				val := response.val
				err := response.err
				numReceived++
				if err != nil {
					// TODO: log the error?
					continue
				}
				values[string(response.nid)] = val
			case <-timeoutEvent:
				break receive
			}
		}

	_, instructions, err := c.store.Reconcile(key, values)
	if err != nil {
		//log something??
	}

	write := func(node ClusterNode, inst *store.Instruction) {
		node.ExecuteWrite(inst.Cmd, inst.Key, inst.Args, inst.Timestamp)
	}

	for nid, instructionList := range instructions {
		node := nodeMap[NodeId(nid)]
		for _, inst := range instructionList {
			go write(node, inst)
		}
	}

}

// executes a read against the cluster
func (c *Cluster) ExecuteRead(
	// the read command to perform
	cmd string,
	// the key to read from
	key string,
	// the command args
	args []string,
	// the consistency level to execute the query at
	consistency ConsistencyLevel,
	// query timeout
	timeout time.Duration,
	// if true, reconciliation should be performed before returning
	synchronous bool,
) (store.Value, error) {

	if !c.store.IsReadCommand(cmd) {
		return nil, fmt.Errorf("Invalid read command: %v", cmd)
	}

	// map of dcid -> []Node
	replicaMap := c.GetNodesForKey(key)
	// map of node ids-> node contacted, used for
	// sending reconciliation corrections
	nodeMap := make(map[NodeId]ClusterNode)
	numNodes := numMappedNodes(replicaMap)
	// used for constructing a response
	responseChannel := make(chan queryResponse, numNodes)
	// used for reconciling all responses
	reconcileChannel := make(chan queryResponse, numNodes)

	// executes the read against the cluster
	execute := func(node ClusterNode) {
		val, err := node.ExecuteRead(cmd, key, args)
		response := queryResponse{nid:node.GetId() , val:val, err:err}
		responseChannel <- response
		reconcileChannel <- response
	}

	// determine if the read only needs to be executed against local nodes
	localOnly := readLocalOnly(consistency)

	// determine how many nodes we need a response from, per datacenter
	// and start querying nodes
	numRequiredResponses := make(map[DatacenterId] int, len(replicaMap))
	for dcid, nodes := range replicaMap {
		if dcid != c.GetDatacenterId() && localOnly {
			numRequiredResponses[dcid] = 0
			continue
		} else {
			switch consistency {
			case CONSISTENCY_ONE:
				numRequiredResponses[dcid] = 1
			case CONSISTENCY_QUORUM, CONSISTENCY_QUORUM_LOCAL:
				numRequiredResponses[dcid] = (len(nodes) / 2) + 1
			case CONSISTENCY_ALL, CONSISTENCY_ALL_LOCAL:
				numRequiredResponses[dcid] = len(nodes)
			case CONSISTENCY_CONSENSUS, CONSISTENCY_CONSENSUS_LOCAL:
				return nil, fmt.Errorf("CONSENSUS consistency not implemented yet")
			default:
				return nil, fmt.Errorf("Unknown consistency level: %v", consistency)
			}
		}

		for _, node := range nodes {
			nodeMap[node.GetId()] = node
			go execute(node)
		}
	}

	// wait for responses
	numReceivedResponses := make(map[DatacenterId] int, len(replicaMap))
	numTotalResponses := 0
	// determines if the number of responses received satisfies the
	// required consistency level
	consistencySatisfied := func() bool {
		for dcid, num := range numRequiredResponses {
			if numReceivedResponses[dcid] < num {
				return false
			}
		}
		return true
	}
	values := make(map[string]store.Value)
	var response queryResponse
	timeoutEvent := time.After(timeout * time.Millisecond)
	for !consistencySatisfied() {
		// too many errors received to satisfy consistency
		if numTotalResponses >= numNodes {
			return nil, fmt.Errorf("Errors received from remote nodes, could not satisfy consistency")
		}

		select {
		case response = <-responseChannel:
			// do something
			val := response.val
			err := response.err
			numTotalResponses++;
			if err != nil {
				// TODO: log the error?
				continue
			}
			// increment number of responses for responding datacenter
			numReceivedResponses[nodeMap[response.nid].GetDatacenterId()]++
			values[string(response.nid)] = val
		case <-timeoutEvent:
			return nil, nodeTimeoutError(fmt.Sprintf("Read not completed before timeout"))
		}
	}

	// reconcile values into a result
	val, _, err := c.store.Reconcile(key, values)
	if err != nil {
		return nil, fmt.Errorf("Error reconciling values: %v", err)
	}

	// repair discrepancies
	repairResponseTimeout := timeout * 2
	if synchronous {
		c.reconcileRead(key, nodeMap, reconcileChannel, repairResponseTimeout)
	} else {
		go c.reconcileRead(key, nodeMap, reconcileChannel, repairResponseTimeout)
	}

	return val, nil
}

// executes a write against the cluster
func (c *Cluster) ExecuteWrite(
	// the read command to perform
	cmd string,
	// the key to write to
	key string,
	// the command args
	args []string,
	// the timestamp to record on the write
	timestamp time.Time,
	// the consistency level to execute the query at
	consistency ConsistencyLevel,
	// query timeout
	timeout time.Duration,
	// if true, reconciliation should be performed before returning
	synchronous bool,
) (store.Value, error) {
	return nil, nil
}

