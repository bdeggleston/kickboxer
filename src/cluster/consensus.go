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
	"code.google.com/p/go-uuid/uuid"
)

import (
	"store"
)

type CommandStatus byte

const (
	DS_IGNORED = CommandStatus(iota)
	DS_PRE_ACCEPTED
	DS_ACCEPTED
	DS_REJECTED
	DS_COMMITTED
	DS_EXECUTED
)

var (
	// timeout receiving the initial quorum of
	// preaccept responses
	PREACCEPT_TIMEOUT_1 = uint64(500)
	// timeout receiving N - 2 preaccept responses
	PREACCEPT_TIMEOUT_2 = uint64(250)
)

// TODO: should consensus operations hijack the timestamp??
// TODO: should reads and writes be collapsed into a single function? Let the store decide what to do?
// TODO: make sure all consensus operations are durably persisted before continuing execution

type CommandID string

func NewCommandID() CommandID {
	return CommandID(uuid.NewUUID().String())
}

// TODO: add predicate
type Command struct {
	ID CommandID

	// the node id of the command leader
	LeaderID NodeId

	// the sequence number of this command (like an array index)
	Sequence uint64

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

	// indicates that the dependencies from the leader
	// matched the replica's local dependencies. This
	// is used when there are only 3 replicas and another
	// replica takes over leadership for a command.
	// Even if the old command leader is unreachable,
	// the new leader will know that at least 2 replicas
	// had identical dependency graphs at the time of proposal,
	// it may still be useful in other situations
	dependencyMatch bool

	instance *Instance
}

// sets the status on this command, and persists it
func (c *Command) setStatus(status CommandStatus) error {
	c.Status = status
	// TODO: durably persist
	return nil
}

// relaxed equality check, it ignores node specific
// information
func (c *Command) RelaxedEqual(o *Command) bool {
	result := true
	result = result && c.ID == o.ID
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

// determines if 2 commands are exactly equal
func (c *Command) Equal(o *Command) bool {
	result := c.RelaxedEqual(o)
	result = result && c.Status == o.Status
	result = result && c.commitTimeout.Equal(o.commitTimeout)
	result = result && c.dependencyMatch == o.dependencyMatch

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

func (d Dependencies) GetMaxSequence() uint64 {
	var seq uint64
	for _, dep := range d {
		if dep.Sequence > seq {
			seq = dep.Sequence
		}
	}
	return seq
}

func (d Dependencies) RelaxedEqual(o Dependencies) bool {
	if len(d) != len(o) {
		return false
	}
	for i := range d {
		if !d[i].RelaxedEqual(o[i]) {
			return false
		}
	}
	return true
}

func (d Dependencies) Equal(o Dependencies) bool {
	if len(d) != len(o) {
		return false
	}
	for i := range d {
		if !d[i].Equal(o[i]) {
			return false
		}
	}
	return true
}

// manager for interfering commands
// Dependencies should be sorted with the
// newest commands at the end of the array
// TODO: rethink the locking strategy. Maybe only the cmdLock should be used?
type Instance struct {
	Dependencies Dependencies
	MaxBallot    uint64
	key          string
	lock         *sync.RWMutex
	cmdLock		 *sync.Mutex

	// allows quick updating of dependency status
	DependencyMap map[CommandID] *Command

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

// persists this instance's state
func (i *Instance) Persist() error {
	// TODO: this
	return nil
}

func (i *Instance) getNextBallot() uint64 {
	i.lock.Lock()
	defer i.lock.Unlock()

	i.MaxBallot++
	return i.MaxBallot
}

func (i *Instance) getNextSequence() uint64 {
	i.lock.RLock()
	defer i.lock.RUnlock()
	var seq uint64
	for _, dep := range i.Dependencies {
		if dep.Sequence > seq { seq = dep.Sequence }
	}
	return seq
}

// adds a command to the dependency list, sets the sequence number
// on the new dependency if it hasn't been set, and returns the old
// set of dependencies.
// If checkDeps is not nil, this instance's dependencies will be
// compared against the provided dependencies, and dependencyMatch will
// be set to true if they're equal
func (i *Instance) addDependency(cmd *Command, checkDeps Dependencies) (old Dependencies, new Dependencies, err error) {
	i.lock.Lock()
	defer i.lock.Unlock()
	old := i.Dependencies.Copy()

	// setup dependency
	cmd.Sequence = old.GetMaxSequence() + 1
	cmd.instance = i

	// if the provided dependencies match the local dependencies, record it
	if checkDeps != nil {
		cmd.dependencyMatch = i.Dependencies.RelaxedEqual(checkDeps)
	}

	i.Dependencies = append(i.Dependencies, cmd)
	i.DependencyMap[cmd.ID] = cmd

	if err := i.Persist(); err != nil {
		return nil, nil, err
	}
	new := i.Dependencies.Copy()

	return old, new, nil
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
func (i *Instance) getReplicas(cl ConsistencyLevel) (replicas []*RemoteNode, err error) {
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
		if len(replicas) != numReplicas - 1 {
			return []*RemoteNode{}, fmt.Errorf("Expected %v replicas, got %v", (numReplicas - 1), len(replicas))
		}

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
		if len(replicas) != numReplicas - 1 {
			return []*RemoteNode{}, fmt.Errorf("Expected %v replicas, got %v", (numReplicas - 1), len(replicas))
		}

	default:
		return []*RemoteNode{}, fmt.Errorf("Unknown consistency level: %v", cl)
	}
	return replicas, nil
}

func (i *Instance) sendPreAccept(replicas []*RemoteNode, cmd *Command, deps Dependencies, ballot uint64) (chan Message, error) {
	msg := &PreAcceptRequest{
		Command: cmd,
		Dependencies:deps,
		Ballot: ballot,
	}


	// send the pre-accept requests
	preAcceptChannel := make(chan Message, len(replicas))
	sendPreAccept := func(node *RemoteNode) {
		response, _, err := node.sendMessage(msg)
		if err != nil {
			logger.Warning("Error receiving PreAcceptResponse: %v", err)
		}
		preAcceptChannel <- response
	}
	for _, node := range replicas {
		go sendPreAccept(node)
	}

	return preAcceptChannel, nil
}

func (i *Instance) getPreAcceptResponses(recvChan <-chan Message, replicaCount int) ([]*PreAcceptResponse, error) {
	quorumSize := (replicaCount / 2) + 1
	numResponses := 1  // this node counts as a response
	responses := make([]*PreAcceptResponse, 0, replicaCount - 1)

	recvResponse := func(msg Message) error {
		switch response := msg.(type) {
		case *PreAcceptResponse:
			responses = append(responses, response)
			numResponses++
		case *BallotRejectResponse:
			panic("Ballot rejection handling not implemented yet")
		default:
			return fmt.Errorf("Unexpected PreAccept response type: %T", msg)
		}
		return nil
	}

	// receive pre-accept responses until quorum is met, or until timeout
	timeoutEvent := time.After(time.Duration(PREACCEPT_TIMEOUT_1) * time.Millisecond)
	var msg Message
	for numResponses < quorumSize {
		select {
		case msg = <- recvChan:
			if err := recvResponse(msg); err != nil { return nil, err }
		case <-timeoutEvent:
			return nil, fmt.Errorf("Timeout while awaiting pre accept responses")
		}
	}
	// receive up to n - 2 responses
	timeoutEvent := time.After(time.Duration(PREACCEPT_TIMEOUT_2) * time.Millisecond)
	receive2: for numResponses < (replicaCount - 2) {
		select {
		case msg = <-recvChan:
			if err := recvResponse(msg); err != nil { return nil, err }
		case <-timeoutEvent:
			break receive2
		}
	}

	// finally, receive any additional responses
	drain: for {
		select {
		case msg = <-recvChan:
			if err := recvResponse(msg); err != nil { return nil, err }
		default:
			break drain
		}
	}
	return responses, nil
}

// perform union on external dependencies
func (i *Instance) dependencyUnion(extDeps []Dependencies) (Dependencies, bool, error) {
//	depListMap := make(map[CommandID] Dependencies)
//	_ = newDeps
//	for _, response := range responses {
//		for _, dep := range response.Dependencies {
//			depList, exists := depListMap[dep.ID]
//			if !exists {
//				depList = make(Dependencies, 0, len(responses))
//				depListMap[dep.ID] = depList
//			}
//			depList = append(depList, dep)
//		}
//	}
	return nil, false, nil
}

func (i *Instance) ExecuteInstruction(inst store.Instruction, cl ConsistencyLevel) (store.Value, error) {
	replicas, err := i.getReplicas(cl)
	if err != nil { return nil, err }

	// calculate the number of all replicase, including this node
	replicaCount := len(replicas) + 1
	quorumSize := (replicaCount / 2) + 1
	_ = quorumSize

	// a lock is aquired here to prevent concurrent operations on this node from
	// interfering with each other. For example, if multiple client queries arrive at
	// this node at effectively the same time, and they arrive at remote nodes out of
	// order, the competing commands will consistently 'one-up' each other, and most
	// client queries will fail
	i.cmdLock.Lock()

	// instantiate the command we'd like to commit
	cmd := &Command{
		ID:		   NewCommandID(),
		LeaderID:  i.cluster.GetNodeId(),
		//Sequence:  i.getNextSequence(),  // this is handled by addDependency
		Status:    DS_PRE_ACCEPTED,
		Cmd:       inst.Cmd,
		Key:       inst.Key,
		Args:      inst.Args,
		Timestamp: inst.Timestamp,
		Blocking:  i.cluster.store.ReturnsValue(inst.Cmd),
	}

	ballot := i.getNextBallot()
	oldDeps, newDeps, err := i.addDependency(cmd, nil)
	if err != nil { return nil, err }
	responseChan, err := i.sendPreAccept(replicas, cmd, oldDeps, ballot)
	// unblock any pending queries on this instance
	i.cmdLock.Unlock()

	// otherwise, resolve the dependencies and force an accept
	extDeps := make([]Dependencies, len(responses))
	for i, response := range responses {
		extDeps[i] = response.Dependencies
	}
	resolvedDeps, err := i.dependencyUnion(extDeps)
	if !newDeps.Equal(resolvedDeps) {
		// TODO: update the instance's dependency list
		// TODO: execute multi-paxos accept phase
	}

	if err := cmd.setStatus(DS_COMMITTED); err != nil {
		return nil, err
	}
	commitMessage := &CommitRequest{cmd.ID}
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
}

func (i *Instance) HandlePreAccept(msg *PreAcceptRequest) (*PreAcceptResponse, error) {
	cmd := msg.Command

	// no need to persist this yet, or use locking, this is the only
	// place this command has been seen, and consensus state persistence
	// will happen in `addDependency`
	cmd.Status = DS_PRE_ACCEPTED

	cmd.dependencyMatch = i.Dependencies.Equal(msg.Dependencies)

	_, deps, err := i.addDependency(cmd, msg.Dependencies)
	if err != nil { return nil, err }

	return &PreAcceptResponse{NodeId: i.cluster.GetNodeId(), Dependencies: deps}, nil
}

func (i *Instance) HandleCommit(msg *CommitRequest) (*CommitResponse, error) {
	return nil, nil
}

func (i *Instance) HandleAccept(msg *AcceptRequest) (*AcceptResponse, error) {
	return nil, nil
}

func (i *Instance) HandleMessage(msg BallotMessage) (BallotMessage, error) {
	// check the ballot first
	ballotReject := func() *BallotRejectResponse {
		i.lock.Lock()
		defer i.lock.Unlock()
		if msg.GetBallot() <= i.MaxBallot {
			return &BallotRejectResponse{Ballot:i.MaxBallot}
		} else {
			i.MaxBallot = msg.GetBallot()
		}
		return nil
	}()
	if ballotReject != nil {
		return ballotReject, nil
	}

	// then continue
	switch request := msg.(type) {
	case *PreAcceptRequest:
		return i.HandlePreAccept(request)
	case *CommitRequest:
		return i.HandleCommit(request)
	case *AcceptRequest:
		return i.HandleAccept(request)
	default:
		return nil, fmt.Errorf("Unexpected message type: %T", msg)
	}
	panic("unreachable")
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
