package topology

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
)

import (
	"node"
	"partitioner"
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

// encapsulates all of the ring get/mutate logic
type Ring struct {
	lock *sync.RWMutex

	// map of node ids to node objects
	nodeMap map[node.NodeId] Node

	// nodes ordered by token
	tokenRing []Node

	// the state of the ring before the most recent ring mutation
	priorRing []Node
}

// creates and starts a ring
func NewRing() *Ring {
	return &Ring{
		nodeMap:make(map[node.NodeId] Node),
		tokenRing:make([]Node, 0),
		priorRing:make([]Node, 0),
		lock:&sync.RWMutex{},
	}
}

func (r *Ring) Size() int {
	return len(r.tokenRing)
}

func (r *Ring) getNode(nid node.NodeId) (Node, error) {
	node, ok := r.nodeMap[nid]
	if !ok {
		return nil, fmt.Errorf("No node found by node id: %v", nid)
	}
	return node, nil

}

// gets a node by it's node id
func (r *Ring) GetNode(nid node.NodeId) (Node, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	node, ok := r.nodeMap[nid]
	if !ok {
		return nil, fmt.Errorf("No node found by node id: %v", nid)
	}
	return node, nil
}

// refreshes the token ring after changes
// this method does no locking, the caller
// needs to do that
func (r *Ring) refreshRing() {
	nodes := make([]Node, len(r.nodeMap))
	idx := 0
	for _, v := range r.nodeMap {
		nodes[idx] = v
		idx++
	}

	// sort by their tokens
	sorter := &nodeSorter{nodes:nodes}
	sort.Sort(sorter)

	// update the ring
	r.priorRing = r.tokenRing
	r.tokenRing = sorter.nodes

}

// adds a node to the ring, returns true if the node
// was added, false if not
func (r *Ring) AddNode(n Node) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	nid := n.GetId()
	_, ok := r.nodeMap[nid]
	if ok {
		return fmt.Errorf("This node is already a part of the ring")
	}

	r.nodeMap[nid] = n
	r.refreshRing()
	return nil
}

// returns a copy of the token ring
func (r *Ring) AllNodes() []Node {
	r.lock.RLock()
	defer r.lock.RUnlock()
	//
	nodes := make([]Node, len(r.tokenRing), len(r.tokenRing))
	for i, n := range r.tokenRing {
		nodes[i] = n
	}
	return nodes
}

// returns the nodes that replicate the given token
// includes the node that owns the token, and it's replicas
//
// to simplify the binary search logic, a token belongs the first
// node with a token greater than or equal to it
// values are replicated forward in the ring
func (r *Ring) GetNodesForToken(t partitioner.Token, replicationFactor uint32) []Node {
	r.lock.RLock()
	defer r.lock.RUnlock()

	numNodes := int(replicationFactor)
	ringLen := len(r.tokenRing)
	if ringLen < int(replicationFactor) {
		numNodes = len(r.tokenRing)
	}
	nodes := make([]Node, numNodes)

	// this will return the first node with a token greater than
	// the given token
	searcher := func(i int) bool {
		return bytes.Compare(t, r.tokenRing[i].GetToken()) <= 0
	}
	idx := sort.Search(ringLen, searcher)

	for i:=0;i<numNodes;i++ {
		nodes[i] = r.tokenRing[(idx + i) % ringLen]
	}
	return nodes
}

