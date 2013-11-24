package cluster

import (
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

func (dc *DatacenterContainer) GetNodesForToken(t Token, replicationFactor uint32) []Node {
	dc.lock.RLock()
	defer dc.lock.RUnlock()
	nodes := make([]Node, 0, int(replicationFactor) * len(dc.rings))
	for _, ring := range dc.rings {
//		nodes = nodes + ring.GetNodesForToken(t, replicationFactor)
		_ = ring
	}

	return nodes
}

