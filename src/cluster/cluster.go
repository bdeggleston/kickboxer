/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 10/2/13
 * Time: 6:47 PM
 * To change this template use File | Settings | File Templates.
 */
package cluster

import (
//	"net"
)

type Token []byte

type Cluster struct {
	// nodes addressed to communicate with to
	// discover the rest of the cluster
	seeds []string

	// the number of nodes a key should
	// be replicated to
	replicationFactor uint32

	localNode LocalNode

	// map of node ids to node objects
	nodeMap map[NodeId] *Node

	// nodes ordered by token
	tokenRing [] *Node

}

func NewCluster() (*Cluster, error) {
	return nil, nil
}

func (c* Cluster) Start() error {
	return nil
}

func (c* Cluster) Stop() error {
	return nil
}
