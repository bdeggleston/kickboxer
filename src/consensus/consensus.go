/*
Kickboxer's egalitarian paxos implementation

http://www.pdl.cmu.edu/PDL-FTP/associated/CMU-PDL-12-108.pdf
http://sigops.org/sosp/sosp13/papers/p358-moraru.pdf
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
