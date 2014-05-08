package consensus

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

import (
	"node"
	"serializer"
	"store"
	"types"
)

type InstanceStatus byte

const (
	_ = InstanceStatus(iota)
	INSTANCE_PREACCEPTED
	INSTANCE_ACCEPTED
	INSTANCE_COMMITTED
	INSTANCE_EXECUTED
)

func (s InstanceStatus) String() string {
	switch s {
	case INSTANCE_PREACCEPTED:
		return "INSTANCE_PREACCEPTED"
	case INSTANCE_ACCEPTED:
		return "INSTANCE_ACCEPTED"
	case INSTANCE_COMMITTED:
		return "INSTANCE_COMMITTED"
	case INSTANCE_EXECUTED:
		return "INSTANCE_EXECUTED"
	default:
		return fmt.Sprintf("Unknown InstanceStatus: %v", byte(s))
	}
	panic("unreachable")
}

type InstanceID struct {
	types.UUID
}

func NewInstanceID() InstanceID {
	return InstanceID{types.NewUUID1()}
}

type InstanceResult struct {
	val store.Value
	err error
}

type InstanceResultChan (chan InstanceResult)

// returns a new InstanceResultChan
func NewInstanceResultChan() InstanceResultChan {
	// make the channel buffered by 1 so
	// the executor doesn't block forever
	// if no one is listening on the channel
	// yet / anymore
	return make(chan InstanceResult, 1)
}

type DepsLog struct {
	Reason string
	Deps []InstanceID
}

// a serializable set of instructions
type Instance struct {
	// the uuid of this instance
	InstanceID InstanceID

	// the node id of the instance leader
	LeaderID node.NodeId

	// the order other nodes handle prepare phases
	Successors []node.NodeId

	// the Instruction to be executed
	Command store.Instruction

	// a list of other instance ids that
	// execution of this instance depends on
	Dependencies []InstanceID

	// the current status of this instance
	Status InstanceStatus

	// the highest seen message number for this instance
	MaxBallot uint32

	// indicates that the paxos protocol
	// for this instance failed, and this
	// instance should be ignored
	Noop bool

	// indicates that the dependencies from the leader
	// matched the replica's local dependencies. This
	// is used when there are only 3 replicas and another
	// replica takes over leadership for a command.
	// Even if the old command leader is unreachable,
	// the new leader will know that at least 2 replicas
	// had identical dependency graphs at the time of proposal,
	// it may still be useful in other situations
	// * not message serialized *
	DependencyMatch bool

	// indicates that this instance only reads data, and only
	// depends on writes. If false, it will depend on reads
	// and writes
	ReadOnly bool

	// channels that clients are waiting on for results
	ResultListeners []InstanceResultChan

	// log of dependency changes, used for debugging
	DepsLog []DepsLog

	// the set of strongly connected instances for this
	// instance. This allows the execution ordering to
	// avoid visiting every instance since the beginning
	// of time, but also prevent reordering of large,
	// partially executed strongly connected subgraphs
	// * not message serialized *
	StronglyConnected InstanceIDSet

	// indicates the time that we can stop waiting
	// for a commit on this command, and force one
	// * not message serialized *
	commitTimeout time.Time

	// indicates the time that we can stop waiting for the
	// the command to be executed by ExecuteQuery
	// * not message serialized *
	executeTimeout time.Time

	// locking
	lock sync.RWMutex

	// lock preventing multiple threads from
	// running a prepare phase simultaneously
	prepareLock sync.Mutex

	// events that wait on commit/execute so dependent
	// threads are notified immediately
	commitEvent *event
	executeEvent *event

	// the instance's parent manager
	manager *Manager
}

func (i *Instance) getCommitEvent() *event {
	i.lock.Lock()
	defer i.lock.Unlock()
	if i.commitEvent == nil {
		i.commitEvent = newEvent()
	}
	return i.commitEvent
}

func (i *Instance) broadcastCommitEvent() {
	i.lock.RLock()
	defer i.lock.RUnlock()
	if i.commitEvent != nil {
		i.commitEvent.broadcast()
	}
}

func (i *Instance) getExecuteEvent() *event {
	i.lock.Lock()
	defer i.lock.Unlock()
	if i.executeEvent == nil {
		i.executeEvent = newEvent()
	}
	return i.executeEvent
}

func (i *Instance) broadcastExecuteEvent() {
	i.lock.RLock()
	defer i.lock.RUnlock()
	if i.executeEvent != nil {
		i.executeEvent.broadcast()
	}
}

func (i *Instance) getExecuteTimeout() time.Time {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.executeTimeout
}

func (i *Instance) getExecuteTimeoutEvent() <-chan time.Time {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return getTimeoutEvent(i.executeTimeout.Sub(time.Now()))
}

func (i *Instance) getCommitTimeout() time.Time {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.commitTimeout
}

func (i *Instance) getCommitTimeoutEvent() <-chan time.Time {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return getTimeoutEvent(i.commitTimeout.Sub(time.Now()))
}

func (i *Instance) getStatus() InstanceStatus {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.Status
}

func (i *Instance) getBallot() uint32 {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.MaxBallot
}

func (i *Instance) getSuccessors() []node.NodeId {
	i.lock.RLock()
	defer i.lock.RUnlock()
	result := make([]node.NodeId, len(i.Successors))
	copy(result, i.Successors)
	return result
}

func (i *Instance) getDependencies() []InstanceID {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.Dependencies
}


func (i *Instance) incrementBallot() uint32 {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.MaxBallot++
	return i.MaxBallot
}

func (i *Instance) updateBallot(ballot uint32) bool {
	i.lock.Lock()
	defer i.lock.Unlock()
	if ballot > i.MaxBallot {
		i.MaxBallot = ballot
		return true
	}
	return false
}

func (i *Instance) setDeps(reason string, deps []InstanceID) {
	i.Dependencies = deps
	if PAXOS_DEBUG {
		if i.DepsLog == nil {
			i.DepsLog = make([]DepsLog, 0)
		}
		entry := DepsLog{Reason: reason, Deps: deps}
		i.DepsLog = append(i.DepsLog, entry)
	}
}

func (i *Instance) setStronglyConnectedIds(iids []InstanceID) {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.StronglyConnected = NewInstanceIDSet(iids)
}

func (i *Instance) addListener() InstanceResultChan {
	i.lock.Lock()
	defer i.lock.Unlock()
	listener := NewInstanceResultChan()
	i.ResultListeners = append(i.ResultListeners, listener)
	return listener
}

// returns a copy of the instance
func (i *Instance) Copy() (*Instance, error) {
	i.lock.RLock()
	defer i.lock.RUnlock()

	newInst := &Instance{
		InstanceID: i.InstanceID,
		LeaderID: i.LeaderID,
		Successors: make([]node.NodeId, len(i.Successors)),
		Command: i.Command.Copy(),
		Dependencies: make([]InstanceID, len(i.Dependencies)),
		Status: i.Status,
		MaxBallot: i.MaxBallot,
		Noop: i.Noop,
		DependencyMatch: i.DependencyMatch,
		commitTimeout: i.commitTimeout,
		executeTimeout: i.executeTimeout,
		manager: i.manager,
	}
	copy(newInst.Successors, i.Successors)
	copy(newInst.Dependencies, i.Dependencies)

	return newInst, nil
}

// merges sequence and dependencies onto this instance, and returns
// true/false to indicate if there were any changes
func (i *Instance) mergeAttributes(deps []InstanceID) (bool, error) {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.Status != INSTANCE_PREACCEPTED {
		return false, fmt.Errorf("attributes can only be merged on instances with preaccepted status, %v", i.Status)
	}
	changes := false
	iSet := NewInstanceIDSet(i.Dependencies)
	oSet := NewInstanceIDSet(deps)
	if !iSet.Equal(oSet) {
		changes = true
		union := iSet.Union(oSet)
		deps = make([]InstanceID, 0, len(union))
		for id := range union {
			deps = append(deps, id)
		}
		i.setDeps("merge attrs", deps)
	}
	return changes, nil
}

// -------------- state changes --------------

func (i *Instance) preaccept(inst *Instance, incrementBallot bool) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.Status > INSTANCE_PREACCEPTED {
		return NewInvalidStatusUpdateError(i, INSTANCE_PREACCEPTED)
	}

	if inst != nil && inst != i {
		inst.lock.RLock()
		i.Noop = inst.Noop
		if inst.MaxBallot > i.MaxBallot {
			i.MaxBallot = inst.MaxBallot
		}
		inst.lock.RUnlock()
	}
	i.Status = INSTANCE_PREACCEPTED
	if deps, err := i.manager.getInstanceDeps(i); err != nil {
		return err
	} else {
		i.setDeps("preaccept", deps)
	}
	i.commitTimeout = makePreAcceptCommitTimeout()
	if incrementBallot {
		i.MaxBallot++
	}
	return nil
}

func (i *Instance) accept(inst *Instance, incrementBallot bool) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.Status > INSTANCE_ACCEPTED {
		logger.Debug("Accept: Can't accept instance %v with status %v", inst.InstanceID, inst.Status)
		return NewInvalidStatusUpdateError(i, INSTANCE_ACCEPTED)
	}

	if inst != nil && inst != i {
		inst.lock.RLock()
		i.setDeps("accept", inst.Dependencies)

		i.Noop = inst.Noop
		if inst.MaxBallot > i.MaxBallot {
			i.MaxBallot = inst.MaxBallot
		}
		inst.lock.RUnlock()
	}
	i.Status = INSTANCE_ACCEPTED
	i.commitTimeout = makeAcceptCommitTimeout()
	if incrementBallot {
		i.MaxBallot++
	}
	return nil
}

// commits the instance
// the broadcast event is not sent here because the
// instance needs to be persisted before the event is sent
func (i *Instance) commit(inst *Instance, incrementBallot bool) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.Status > INSTANCE_COMMITTED {
		logger.Debug("Commit: Can't commit instance %v with status %v", inst.InstanceID, inst.Status)
		return NewInvalidStatusUpdateError(i, INSTANCE_COMMITTED)
	} else if PAXOS_DEBUG && i.Status == INSTANCE_COMMITTED {
		if !NewInstanceIDSet(i.Dependencies).Equal(NewInstanceIDSet(inst.Dependencies)) {
			logger.Critical("%v already committed with different seq/deps", i.InstanceID)
		}
	}

	if inst != nil && inst != i {
		inst.lock.RLock()
		// this replica may have missed an accept message
		// so copy the seq & deps onto the existing instance
		i.setDeps("commit", inst.Dependencies)

		i.Noop = inst.Noop
		if inst.MaxBallot > i.MaxBallot {
			i.MaxBallot = inst.MaxBallot
		}
		inst.lock.RUnlock()
	}
	i.Status = INSTANCE_COMMITTED
	i.executeTimeout = makeExecuteTimeout()

	if incrementBallot {
		i.MaxBallot++
	}
	return nil
}

func (i *Instance) setNoop() {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.Noop = true
}

// -------------- serialization --------------

func (i *Instance) NumBytesLimitedUnsafe() int {
	var numBytes int

	// instance id
	numBytes += types.UUID_NUM_BYTES

	// leader id
	numBytes += types.UUID_NUM_BYTES

	// successors
	numBytes += 4  // num successors header
	numBytes += types.UUID_NUM_BYTES * len(i.Successors)

	// instructions
	numBytes += i.Command.NumBytes()

	// dependencies
	numBytes += 4  // num dependencies header
	numBytes += types.UUID_NUM_BYTES * len(i.Dependencies)

	// status
	numBytes += 1

	// ballot
	numBytes += 4

	// noop
	numBytes += 1

	// match
	numBytes += 1

	// read only
	numBytes += 1

	return numBytes
}

func (i *Instance) NumBytesLimited() int {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.NumBytesLimitedUnsafe()
}

func (i *Instance) SerializeLimitedUnsafe(buf *bufio.Writer) error {
	if err := (&i.InstanceID).WriteBuffer(buf); err != nil { return err }

	if err := (&i.LeaderID).WriteBuffer(buf); err != nil { return err }
	numSuccessors := uint32(len(i.Successors))
	if err := binary.Write(buf, binary.LittleEndian, &numSuccessors); err != nil { return err }
	for idx := range i.Successors {
		if err := (&i.Successors[idx]).WriteBuffer(buf); err != nil { return err }
	}

	if err := i.Command.Serialize(buf); err != nil { return nil}

	numDeps := uint32(len(i.Dependencies))
	if err := binary.Write(buf, binary.LittleEndian, &numDeps); err != nil { return err }
	for idx := range i.Dependencies {
		if err := (&i.Dependencies[idx]).WriteBuffer(buf); err != nil { return err }
	}

	if err := binary.Write(buf, binary.LittleEndian, &i.Status); err != nil { return err }
	if err := binary.Write(buf, binary.LittleEndian, &i.MaxBallot); err != nil { return err }

	var noop byte
	if i.Noop { noop = 0xff }
	if err := binary.Write(buf, binary.LittleEndian, &noop); err != nil { return err }

	var match byte
	if i.DependencyMatch { match = 0xff }
	if err := binary.Write(buf, binary.LittleEndian, &match); err != nil { return err }

	var readonly byte
	if i.ReadOnly { readonly = 0xff }
	if err := binary.Write(buf, binary.LittleEndian, &readonly); err != nil { return err }

	return nil
}

func (i *Instance) DeserializeLimited(buf *bufio.Reader) error {
	if err := (&i.InstanceID).ReadBuffer(buf); err != nil { return err }
	if err := (&i.LeaderID).ReadBuffer(buf); err != nil { return err }

	var numSuccessors uint32
	if err := binary.Read(buf, binary.LittleEndian, &numSuccessors); err != nil { return err }
	i.Successors = make([]node.NodeId, numSuccessors)
	for idx := range i.Successors {
		if err := (&i.Successors[idx]).ReadBuffer(buf); err != nil { return err }
	}

	if err := i.Command.Deserialize(buf); err != nil { return err }

	var numDeps uint32
	if err := binary.Read(buf, binary.LittleEndian, &numDeps); err != nil { return err }
	deps := make([]InstanceID, numDeps)
	for idx := range deps {
		if err := (&deps[idx]).ReadBuffer(buf); err != nil { return err }
	}
	i.setDeps("deserialize", deps)

	if err := binary.Read(buf, binary.LittleEndian, &i.Status); err != nil { return err }
	if err := binary.Read(buf, binary.LittleEndian, &i.MaxBallot); err != nil { return err }

	var noop byte
	if err := binary.Read(buf, binary.LittleEndian, &noop); err != nil { return err }
	i.Noop = noop != 0x0

	var match byte
	if err := binary.Read(buf, binary.LittleEndian, &match); err != nil { return err }
	i.DependencyMatch = match != 0x0

	var readonly byte
	if err := binary.Read(buf, binary.LittleEndian, &readonly); err != nil { return err }
	i.ReadOnly = readonly != 0x0

	return nil
}

func (i *Instance) SerializeLimited(buf *bufio.Writer) error {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.SerializeLimitedUnsafe(buf)
}

func (i *Instance) NumBytes() int {
	i.lock.RLock()
	defer i.lock.RUnlock()
	numBytes := i.NumBytesLimitedUnsafe()

	// commit and execute timeouts
	numBytes += serializer.NumTimeBytes() * 2

	return numBytes
}

func (i *Instance) Serialize(buf *bufio.Writer) error {
	i.lock.RLock()
	defer i.lock.RUnlock()
	if err := i.SerializeLimitedUnsafe(buf); err != nil { return err }
	if err := serializer.WriteTime(buf, i.commitTimeout); err != nil { return err }
	if err := serializer.WriteTime(buf, i.executeTimeout); err != nil { return err }

	return nil
}

func (i *Instance) Deserialize(buf *bufio.Reader) error {
	if err := i.DeserializeLimited(buf); err != nil { return err }
	if val, err := serializer.ReadTime(buf); err != nil { return err } else {
		i.commitTimeout = val
	}
	if val, err := serializer.ReadTime(buf); err != nil { return err } else {
		i.executeTimeout = val
	}
	return nil
}

