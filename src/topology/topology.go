/**
Contains interfaces and structs used for managing cluster topology
*/
package topology

import (
	"fmt"
	"sync"
)

import (
	"node"
	"partitioner"
)

// contains all of the logic and data
// for answering questions about the current
// topology, and routing keys to nodes
type Topology struct {
	partitioner       partitioner.Partitioner
	localNodeID       node.NodeId
	localDcID         DatacenterID
	replicationFactor uint

	rings map[DatacenterID]*Ring
	nodes map[node.NodeId]Node
	lock  sync.RWMutex
}

func NewTopology(
	localNID node.NodeId,
	localDCID DatacenterID,
	prtnr partitioner.Partitioner,
	replicationFactor uint,
) *Topology {
	return &Topology{
		localNodeID:       localNID,
		localDcID:         localDCID,
		partitioner:       prtnr,
		replicationFactor: replicationFactor,
		rings: make(map[DatacenterID]*Ring, 1),
		nodes: make(map[node.NodeId]Node, 1),
	}
}

func (t *Topology) AddNode(n Node) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	dcId := n.GetDatacenterId()
	if _, exists := t.rings[dcId]; !exists {
		t.rings[dcId] = NewRing()
	}
	err := t.rings[dcId].AddNode(n)
	if err == nil {
		t.nodes[n.GetId()] = n
	}
	return err
}

func (t *Topology) Size() int {
	t.lock.RLock()
	defer t.lock.RUnlock()

	num := 0
	for _, ring := range t.rings {
		num += ring.Size()
	}
	return num
}

func (t *Topology) AllNodes() []Node {
	t.lock.RLock()
	defer t.lock.RUnlock()

	nodes := make([]Node, 0, t.Size())
	for _, ring := range t.rings {
		nodes = append(nodes, ring.AllNodes()...)
	}
	return nodes
}

func (t *Topology) AllLocalNodes() []Node {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.rings[t.localDcID].AllNodes()
}

func (t *Topology) GetRing(dcId DatacenterID) (*Ring, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	ring, exists := t.rings[dcId]
	if !exists {
		return nil, fmt.Errorf("Unknown datacenter [%v]", dcId)
	}
	return ring, nil
}

func (t *Topology) GetNode(nid node.NodeId) (Node, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	n, exists := t.nodes[nid]
	if !exists {
		return nil, fmt.Errorf("No node found by node id: %v", nid)
	}
	return n, nil
}

func (t *Topology) GetLocalNodeID() node.NodeId {
	return t.localNodeID
}

func (t *Topology) GetLocalDatacenterID() DatacenterID {
	return t.localDcID
}

func (t *Topology) GetToken(key string) partitioner.Token {
	return t.partitioner.GetToken(key)
}

// returns a map of datacenter ids -> replica nodes
func (t *Topology) GetNodesForToken(tk partitioner.Token) map[DatacenterID][]Node {
	t.lock.RLock()
	defer t.lock.RUnlock()

	// allocate an additional space for the local node when this is used in queries
	nodes := make(map[DatacenterID][]Node, len(t.rings)+1)
	for dcid, ring := range t.rings {
		nodes[dcid] = ring.GetNodesForToken(tk, uint32(t.replicationFactor))
	}

	return nodes
}

// returns local dc replicas for the given token
func (t *Topology) GetLocalNodesForToken(tk partitioner.Token) []Node {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return t.rings[t.localDcID].GetNodesForToken(tk, uint32(t.replicationFactor))
}

// returns true if the given token is replicated by the local node
func (t *Topology) TokenLocallyReplicated(tk partitioner.Token) bool {
	for _, n := range t.GetLocalNodesForToken(tk) {
		if n.GetId() == t.localNodeID {
			return true
		}
	}
	return false
}
