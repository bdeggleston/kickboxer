package cluster

// references:
// http://www.pdl.cmu.edu/PDL-FTP/associated/CMU-PDL-12-108.pdf
// http://sigops.org/sosp/sosp13/papers/p358-moraru.pdf

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
	DS_PRE_ACCEPTED
	DS_ACCEPTED
	DS_REJECTED
	DS_COMMITTED
	DS_EXECUTED
)

var (
	PREACCEPT_TIMEOUT = uint64(10000)
)

// TODO: should consensus operations hijack the timestamp??
// TODO: should reads and writes be collapsed into a single function? Let the store decide what to do?
// TODO: make sure all consensus operations are durably persisted before continuing execution

type CommandID struct {
	// the node id of the command leader
	LeaderID NodeId

	// the ballot number for this commend
	Ballot uint64
}

// TODO: add predicate
type Command struct {
	ID CommandID

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

// sets the status on this command, and persists it
func (c *Command) setStatus(status CommandStatus) error {
	c.Status = status
	// TODO: durably persist
	return nil
}

// determines if 2 commands are equal
func (c *Command) Equal(o *Command) bool {
	result := true
	result = result && c.ID == o.ID
	result = result && c.Status == o.Status
	result = result && c.Cmd == o.Cmd
	result = result && c.Key == o.Key
	result = result && c.Blocking == o.Blocking
	result = result && c.Timestamp.Equal(o.Timestamp)
	if len(c.Args) != len(o.Args) {
		return false
	}
	for i := 0; i < len(c.Args); i++ {
		result = result && c.Args[i] == o.Args[i]
	}
	return result
}

type Dependencies []*Command

// returns an array with a copy of each of
// the dependencies
func (d Dependencies) Copy() Dependencies {
	c := make(Dependencies, len(d))
	for i := range d {
		x := *d[i]
		c[i] = &x
	}
	return c
}

// manager for interfering commands
// Dependencies should be sorted with the
// newest commands at the end of the array
type Instance struct {
	Dependencies Dependencies
	MaxBallot    uint64
	key          string
	lock         *sync.RWMutex
	cmdLock		 *sync.Mutex

	// allows quick updating of dependency status
	DependencyMap map[NodeId] map[uint64] *Command

	// keeps track of the highest ballots seen from
	// other replicas, allowing this replica to
	// disregard old messages
	MaxBallotMap map[NodeId] uint64

	cluster *Cluster
}

func NewInstance(key string, cluster *Cluster) *Instance {
	return &Instance{
		key:          key,
		cluster:      cluster,
		Dependencies: make(Dependencies, 0, 20),
		lock:         &sync.RWMutex{},
		cmdLock:	  &sync.Mutex{},
	}
}

func (i *Instance) getNextBallot() uint64 {
	i.lock.Lock()
	defer i.lock.Unlock()

	i.MaxBallot++
	return i.MaxBallot
}

func (i *Instance) updateMaxBallot(ballot uint64) uint64 {
	i.lock.Lock()
	defer i.lock.Unlock()
	if ballot > i.MaxBallot {
		i.MaxBallot = ballot
	}
	return i.MaxBallot
}

// executes the command locally, mutating the store,
// waiting for / forcing commits on uncommitted command
// dependencies if the command is a blocking one
func (i *Instance) executeCommand(cmd *Command) (store.Value, error) {
	// TODO: this
	return nil, nil
}

// gets the replicas & quorum size for this instance, for the given consistency level
func (i *Instance) getReplicas(cl ConsistencyLevel) (replicas []*RemoteNode, quorumSize int, err error) {
	switch cl {
	case CONSISTENCY_CONSENSUS_LOCAL:
		localReplicas := i.cluster.GetLocalNodesForKey(i.key)
		numReplicas := len(localReplicas)
		replicas = make([]*RemoteNode, 0, numReplicas-1)

		for _, node := range localReplicas {
			if rnode, ok := node.(*RemoteNode); ok {
				replicas = append(replicas, rnode)
			}
		}
		if len(replicas) != numReplicas-1 {
			return []*RemoteNode{}, 0, fmt.Errorf("Expected %v replicas, got %v", (numReplicas - 1), len(replicas))
		}
		quorumSize = (len(localReplicas) / 2) + 1

	case CONSISTENCY_CONSENSUS:
		replicaMap := i.cluster.GetNodesForKey(i.key)
		numReplicas := 0
		for _, nodes := range replicaMap {
			numReplicas += len(nodes)
		}
		replicas = make([]*RemoteNode, 0, numReplicas-1)
		for _, nodes := range replicaMap {
			for _, node := range nodes {
				if rnode, ok := node.(*RemoteNode); ok {
					replicas = append(replicas, rnode)
				}
			}
		}
		if len(replicas) != numReplicas-1 {
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
	if err != nil {
		return nil, err
	}

	// a lock is aquired here to prevent concurrent operations on this node from
	// interfering with each other. For example, if multiple client queries arrive at
	// this node at effectively the same time, and they arrive at remote nodes out of
	// order, the competing commands will consistently 'one-up' each other, and most
	// client queries will fail
	i.cmdLock.Lock()

	// instantiate the command we'd like to commit
	ballot := i.getNextBallot()
	cmd := &Command{
		ID:		   CommandID{i.cluster.GetNodeId(), ballot},
		Status:    DS_PRE_ACCEPTED,
		Cmd:       inst.Cmd,
		Key:       inst.Key,
		Args:      inst.Args,
		Timestamp: inst.Timestamp,
		Blocking:  i.cluster.store.ReturnsValue(inst.Cmd),
	}

	// lock the instance, copy it's dependencies
	// into the PreAccept message, and add this
	// command into the local dependencies
	msg := func() *PreAcceptRequest {
		i.lock.Lock()
		defer i.lock.Unlock()
		msg := &PreAcceptRequest{
			Command: cmd,
			Dependencies:i.Dependencies.Copy(),
		}
		i.Dependencies = append(i.Dependencies, cmd)
		return msg
	}()

	// send the pre-accept requests
	responses := make([]*PreAcceptResponse, 0, len(replicas))
	preAcceptChannel := make(chan *PreAcceptResponse, len(replicas))
	sendPreAccept := func(node *RemoteNode) {
		response, _, err := node.sendMessage(msg)
		if err != nil {
			logger.Warning("Error receiving PreAcceptResponse: %v", err)
		}
		if msg, ok := response.(*PreAcceptResponse); !ok {
			logger.Warning("Unexpected PreAccept response type: %T\n%+v", response, response)
		} else {
			preAcceptChannel <- msg
		}
	}
	for _, node := range replicas {
		go sendPreAccept(node)
	}

	// receive pre-accept responses until quorum is met, or until timeout
	timeoutEvent := time.After(time.Duration(PREACCEPT_TIMEOUT) * time.Millisecond)
	numResponses := 0
	preAcceptOk := true
	var response *PreAcceptResponse
	for numResponses < quorumSize {
		select {
		case response = <-preAcceptChannel:
			preAcceptOk = preAcceptOk && response.Accepted
			responses = append(responses, response)
		case <-timeoutEvent:
			return nil, fmt.Errorf("Timeout while awaiting pre accept responses")
		}
	}
	// unblock any pending queries on this instance
	i.cmdLock.Unlock()

	// if the quorum ok'd the pre accept, commit it
	if preAcceptOk {
		cmd.Status = DS_COMMITTED
		commitMessage := &CommitRequest{LeaderID:cmd.ID.LeaderID, Ballot:cmd.ID.Ballot}
		sendCommit := func(node *RemoteNode) {
			response, _, err := node.sendMessage(commitMessage)
			if err != nil {
				logger.Warning("Error receiving CommitResponse: %v", err)
			}
			if _, ok := response.(*PreAcceptResponse); !ok {
				logger.Warning("Unexpected Commit response type: %T\n%+v", response, response)
			}
		}
		for _, node := range replicas {
			go sendCommit(node)
		}
		return i.executeCommand(cmd)
	} else {
		// otherwise, resolve the dependencies and force an accept
		// TODO: perform a union of the commands

	}

	return nil, nil
}

func (i *Instance) HandlePreAccept(msg *PreAcceptRequest) (*PreAcceptResponse, error) {
	return nil, nil
}

func (i *Instance) HandleCommit(msg *CommitRequest) (*CommitResponse, error) {
	return nil, nil
}

func (i *Instance) HandleAccept(msg *AcceptRequest) (*AcceptResponse, error) {
	return nil, nil
}

type ConsensusManager struct {
	cluster   *Cluster
	lock      *sync.RWMutex
	instances map[string]*Instance
}

func NewConsensusManager(cluster *Cluster) *ConsensusManager {
	return &ConsensusManager{
		cluster:   cluster,
		instances: make(map[string]*Instance),
		lock:      &sync.RWMutex{},
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
	cm.lock.RLock()
	instance, exists := cm.instances[key]
	cm.lock.RUnlock()

	// or create
	if !exists {
		cm.lock.Lock()
		instance = NewInstance(key, cm.cluster)
		cm.instances[key] = instance
		cm.lock.Unlock()
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
	instance := cm.getInstance(msg.Command.Key)
	_ = instance
	return nil, nil
}

func (cm *ConsensusManager) HandleCommit(msg *CommitRequest) (*CommitResponse, error) {
	return nil, nil
}

func (cm *ConsensusManager) HandleAccept(msg *AcceptRequest) (*AcceptResponse, error) {
	return nil, nil
}
