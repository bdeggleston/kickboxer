/*
Kickboxer's egalitarian paxos implementation
*/
package consensus

import (
	"node"
	"store"
)

import (
	logging "github.com/op/go-logging"
)

var logger *logging.Logger

func init() {
	logger = logging.MustGetLogger("consensus")
}

// defines relevant cluster methods
type NodeCoordinator interface {
	GetID() node.NodeId
	GetStore() store.Store
}
