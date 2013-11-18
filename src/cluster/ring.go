package cluster

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
)

// encapsulates all of the ring get/mutate logic
type Ring struct {
	lock sync.RWMutex

	// map of node ids to node objects
	nodeMap map[NodeId] Node

	// nodes ordered by token
	tokenRing []Node

	// the state of the ring before the most recent ring mutation
	priorRing []Node
}

// creates and starts a ring
func NewRing() *Ring {
	r := &Ring{}
	r.nodeMap = make(map[NodeId] Node)
	r.tokenRing = make([]Node, 0)
	r.priorRing = make([]Node, 0)
	return r
}

func (r *Ring) getNode(nid NodeId) (Node, error) {
	node, ok := r.nodeMap[nid]
	if !ok {
		return nil, fmt.Errorf("No node found by node id: %v", nid)
	}
	return node, nil

}

// gets a node by it's node id
func (r *Ring) GetNode(nid NodeId) (Node, error) {
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
func (r *Ring) AddNode(node Node) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	nid := node.GetId()
	_, ok := r.nodeMap[nid]
	if ok {
		return fmt.Errorf("This node is already a part of the ring")
	}

	r.nodeMap[nid] = node
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
func (r *Ring) GetNodesForToken(t Token, replicationFactor uint32) []Node {
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

