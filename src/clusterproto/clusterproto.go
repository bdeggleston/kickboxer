/*
Defines types and interfaces used by libraries working with the cluster
 */
package clusterproto

import (
	"time"
)

import (
	"node"
	"store"
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

// defines relevant cluster methods
type Cluster interface {
	// returns the id of the cluster
	GetID() node.NodeId

	// returns the cluster's store
	GetStore() store.Store

	// returns the replicas for the given key
	// at the given constistency level
	GetNodesForKey(key string) []node.Node

	// executes a distributed query against cluster
	ExecuteQuery(cmd string, key string, args []string, timestamp time.Time) (store.Value, error)

	// executes a query against the local store
	ApplyQuery(cmd string, key string, args []string, timestamp time.Time) (store.Value, error)

	// TODO: remove and rework epaxos to interact with the store directly
	InterferingKeys(instruction *store.Instruction) []string
}
