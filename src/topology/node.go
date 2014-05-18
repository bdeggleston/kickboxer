package topology

import (
	"node"
	"partitioner"
)

type NodeStatus string

const (
	NODE_INITIALIZING 	= NodeStatus("")
	NODE_UP 			= NodeStatus("UP")
	NODE_DOWN 			= NodeStatus("DOWN")
)

type Node interface {
	node.Node

	Name() string
	GetAddr() string
	GetToken() partitioner.Token
	GetDatacenterId() DatacenterID
	GetStatus() NodeStatus
}
