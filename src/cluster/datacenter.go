package cluster

import (
	"fmt"
	"sync"
)

/**
 What happens when a new datacenter joins?
 * both clusters collect information about each other
 * streaming needs to be reworked to reconcile incoming data

 Cases to handle:
 	* both dcs have data, data needs to be reconciled between both datacenters
 */

/**
 How do datacenter's communicate?
 * All nodes talk to all nodes
 * datacenters pick a random coordinator on each request (why?)
 */

type DatacenterId string

type DatacenterContainer struct {
	rings map[DatacenterId] *Ring
	lock sync.RWMutex
}

func NewDatacenterContainer() *DatacenterContainer {
	dc := &DatacenterContainer{
		rings: make(map[DatacenterId]*Ring),
	}
	return dc
}

func (dc *DatacenterContainer) AddNode(node Node) error {
	dc.lock.Lock()
	defer dc.lock.Unlock()

	dcId := node.GetDatacenterId()
	if _, exists := dc.rings[dcId]; !exists {
		dc.rings[dcId] = NewRing()
	}
	return dc.rings[dcId].AddNode(node)
}

func (dc *DatacenterContainer) GetRing(dcId DatacenterId) (*Ring, error) {
	dc.lock.RLock()
	defer dc.lock.RUnlock()

	ring, exists := dc.rings[dcId]
	if !exists {
		return nil, fmt.Errorf("Unknown datacenter [%v]", dcId)
	}
	return ring, nil
}

func (dc *DatacenterContainer) GetNodesForToken(t Token, replicationFactor uint32) []Node {
	dc.lock.RLock()
	defer dc.lock.RUnlock()

	nodes := make([]Node, 0, int(replicationFactor) * len(dc.rings))
	for _, ring := range dc.rings {
		for _, node := range ring.GetNodesForToken(t, replicationFactor) {
			nodes = append(nodes, node)
		}
	}

	return nodes
}

