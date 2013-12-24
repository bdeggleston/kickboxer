package cluster

import (
	"fmt"
	"sync"
	"time"
)

import (
	"store"
)

type CommandStatus byte

const (
	DS_NULL = CommandStatus(iota)
	DS_PRE_ACCEPT
	DS_ACCEPTED
	DS_REJECTED
	DS_COMMITTED
	DS_EXECUTED
)

// TODO: should consensus operations hijack the timestamp??
// TODO: should reads and writes be collapsed into a single function? Let the store decide what to do?

type Command struct {
	// the node id of the command leader
	LeaderID NodeId

	// the ballot number for this commend
	Ballot uint64

	// the current status of this command
	Status CommandStatus

	// the actual instruction to be executed
	Cmd       string
	Key       string
	Args      []string
	Timestamp time.Time

	// indicates that previous commands need
	// to be executed before a decision can be
	// made for this command
	Blocking bool

	// indicates the time that we can stop waiting
	// for a commit on this command, and force one
	commitTimeout time.Time
}

// determines if 2 commands are equal
func (c *Command) Equal(o *Command) bool {
	result := true
	result = result && c.LeaderID == o.LeaderID
	result = result && c.Status == o.Status
	result = result && c.Cmd == o.Cmd
	result = result && c.Key == o.Key
	result = result && c.Blocking == o.Blocking
	result = result && c.Ballot == o.Ballot
	result = result && c.Timestamp.Equal(o.Timestamp)
	if len(c.Args) != len(o.Args) { return false }
	for i:=0;i<len(c.Args);i++ {
		result = result && c.Args[i] == o.Args[i]
	}
	return result
}

type Dependencies []*Command

// manager for interfering commands
type Instance struct {
	Dependencies Dependencies
	MaxBallot   uint64
	key string

	cluster *Cluster
}

func NewInstance(key string, cluster *Cluster) *Instance {
	return &Instance{
		key:key,
		cluster:cluster,
		Dependencies:make(Dependencies, 0, 20),
	}
}

// gets the replicas & quorum size for this instance, for the given consistency level
func (i *Instance) getReplicas(cl ConsistencyLevel) (replicas []*RemoteNode, quorumSize int, err error) {
	switch cl {
	case CONSISTENCY_CONSENSUS_LOCAL:
		localReplicas := i.cluster.GetLocalNodesForKey(i.key)
		numReplicas := len(localReplicas)
		replicas = make([]*RemoteNode, 0, numReplicas - 1)

		for _, node := range localReplicas {
			if rnode, ok := node.(*RemoteNode); ok {
				replicas = append(replicas, rnode)
			}
		}
		if len(replicas) != numReplicas - 1 {
			return []*RemoteNode{}, 0, fmt.Errorf("Expected %v replicas, got %v", (numReplicas - 1), len(replicas))
		}
		quorumSize = (len(localReplicas) / 2) + 1

	case CONSISTENCY_CONSENSUS:
		replicaMap := i.cluster.GetNodesForKey(i.key)
		numReplicas := 0
		for _, nodes := range replicaMap { numReplicas += len(nodes) }
		replicas = make([]*RemoteNode, 0, numReplicas - 1)
		for _, nodes := range replicaMap {
			for _, node := range nodes {
				if rnode, ok := node.(*RemoteNode); ok {
					replicas = append(replicas, rnode)
				}
			}
		}
		if len(replicas) != numReplicas - 1 {
			return []*RemoteNode{}, 0, fmt.Errorf("Expected %v replicas, got %v", (numReplicas - 1), len(replicas))
		}
		quorumSize = (len(replicas) / 2) + 1

	default:
		return []*RemoteNode{}, 0, fmt.Errorf("Unknown consistency level: %v", cl)
	}
	return replicas, quorumSize, nil
}

func (i *Instance) ExecuteInstruction(inst store.Instruction, cl ConsistencyLevel) (store.Value, error) {
	replicas, quorumSize, err := i.getReplicas(cl)
	_ = replicas
	_ = quorumSize
	if err != nil { return nil, err }
	return nil, nil
}

type ConsensusManager struct {
	cluster   *Cluster
	lock      *sync.RWMutex
	instances map[string]*Instance
}

func NewConsensusManager(cluster *Cluster) *ConsensusManager {
	return &ConsensusManager{
		cluster:cluster,
		instances:make(map[string]*Instance),
	}
}

// determines if the cluster can be the command leader for the given instruction
func (cm *ConsensusManager) canExecute(inst store.Instruction) bool {
	for _, replica := range cm.cluster.GetLocalNodesForKey(inst.Key) {
		if replica.GetId() == cm.cluster.GetNodeId() {
			return true
		}
	}
	return false
}

func (cm *ConsensusManager) getInstance(key string) *Instance {
	// get
	cm.instanceMutex.RLock()
	instance, exists := cm.instances[key]
	cm.instanceMutex.RUnlock()

	// or create
	if !exists {
		cm.instanceMutex.Lock()
		instance = NewInstance(key, cm.cluster)
		cm.instances[key] = instance
		cm.instanceMutex.Unlock()
	}

	return instance
}

func (cm *ConsensusManager) ExecuteInstruction(inst store.Instruction, cl ConsistencyLevel) (store.Value, error) {
	if !cm.canExecute(inst) {
		// need to iterate over the possible replicas, allowing for
		// some to be down
		panic("Forward to eligible replica not implemented yet")
	} else {
		instance := cm.getInstance(inst.Key)
		val, err := instance.ExecuteInstruction(inst, cl)
		return val, err
	}
	return nil, nil
}

func (cm *ConsensusManager) HandlePreAccept(msg *PreAcceptRequest) (*PreAcceptResponse, error) {
	return nil, nil
}

func (cm *ConsensusManager) HandleCommit(msg *CommitRequest) (*CommitResponse, error) {
	return nil, nil
}

func (cm *ConsensusManager) HandleAccept(msg *AcceptRequest) (*AcceptResponse, error) {
	return nil, nil
}

