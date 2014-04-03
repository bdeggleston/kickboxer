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

//func (i InstanceID) UUID() uuid.UUID {
//	return uuid.UUID(i)
//}

type InstanceIDSet map[InstanceID]bool

func NewInstanceIDSet(ids []InstanceID) InstanceIDSet {
	s := make(InstanceIDSet, len(ids))
	for _, id := range ids {
		s[id] = true
	}
	return s
}

func (i InstanceIDSet) Equal(o InstanceIDSet) bool {
	if len(i) != len(o) {
		return false
	}
	for key := range i {
		if !o[key] {
			return false
		}
	}
	return true
}

func (i InstanceIDSet) Union(o InstanceIDSet) InstanceIDSet {
	u := make(InstanceIDSet, (len(i)*3)/2)
	for k := range i {
		u[k] = true
	}
	for k := range o {
		u[k] = true
	}
	return u
}

func (i InstanceIDSet) Add(ids ...InstanceID) {
	for _, id := range ids {
		i[id] = true
	}
}

// returns all of the keys in i, that aren't in o
func (i InstanceIDSet) Subtract(o InstanceIDSet) InstanceIDSet {
	s := NewInstanceIDSet([]InstanceID{})
	for key := range i {
		if !o.Contains(key) {
			s.Add(key)
		}
	}
	return s
}

func (i InstanceIDSet) Contains(id InstanceID) bool {
	_, exists := i[id]
	return exists
}

func (i InstanceIDSet) List() []InstanceID {
	l := make([]InstanceID, 0, len(i))
	for k := range i {
		l = append(l, k)
	}
	return l
}

func (i InstanceIDSet) String() string {
	s := "{"
	n := 0
	for k := range i {
		if n > 0 {
			s += ", "
		}
		s += k.String()
		n++
	}
	s += "}"
	return s
}

type InstanceMap struct {
	instMap map[InstanceID]*Instance
	lock sync.RWMutex
}

func NewInstanceMap() *InstanceMap {
	return &InstanceMap{instMap: make(map[InstanceID]*Instance)}
}

func (i *InstanceMap) Add(instance *Instance) {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.instMap[instance.InstanceID] = instance
}

func (i *InstanceMap) Get(iid InstanceID) *Instance {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.instMap[iid]
}

// gets an existing instance matching the given instance's iid, or adds the
// given instance to the map. The initialize function, if not nil, is run
// on the instance if it's being added to the map, before the map locks
// are released
func (i *InstanceMap) GetOrSet(inst *Instance, initialize func(*Instance)) (*Instance, bool) {
	if instance := i.Get(inst.InstanceID); instance != nil {
		return instance, true
	}

	i.lock.Lock()
	defer i.lock.Unlock()

	instance := i.instMap[inst.InstanceID]
	existed := true
	if instance == nil {
		if initialize != nil {
			initialize(inst)
		}
		i.instMap[inst.InstanceID] = inst
		existed = false
		instance = inst
	}
	return instance, existed
}

func (i *InstanceMap) Len() int {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return len(i.instMap)
}

func (i *InstanceMap) Remove(instance *Instance) {
	i.lock.Lock()
	defer i.lock.Unlock()
	delete(i.instMap, instance.InstanceID)
}

func (i *InstanceMap) RemoveID(id InstanceID) {
	i.lock.Lock()
	defer i.lock.Unlock()
	delete(i.instMap, id)
}

func (i *InstanceMap) ContainsID(id InstanceID) bool {
	i.lock.RLock()
	defer i.lock.RUnlock()
	_, exists := i.instMap[id]
	return exists
}

func (i *InstanceMap) Contains(instance *Instance) bool {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.ContainsID(instance.InstanceID)
}

func (i *InstanceMap) InstanceIDs() []InstanceID {
	i.lock.RLock()
	defer i.lock.RUnlock()
	arr := make([]InstanceID, 0, len(i.instMap))
	for key := range i.instMap {
		arr = append(arr, key)
	}
	return arr
}

func (i *InstanceMap) Instances() []*Instance {
	i.lock.RLock()
	defer i.lock.RUnlock()
	arr := make([]*Instance, 0, len(i.instMap))
	for _, instance := range i.instMap {
		arr = append(arr, instance)
	}
	return arr
}

// adds the instances of the given instance ids to the given instance map, and returns it
// if the given map is nil, a new map is allocated and returned
func (i *InstanceMap) GetMap(imap map[InstanceID]*Instance, iids []InstanceID) map[InstanceID]*Instance {
	if imap == nil {
		imap = make(map[InstanceID]*Instance, len(iids))
	}

	i.lock.RLock()
	defer i.lock.RUnlock()

	for _, iid := range iids {
		imap[iid] = i.instMap[iid]
	}

	return imap
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

	// the sequence number of this instance (like an array index)
	Sequence uint64

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

func (i *Instance) getSeq() uint64 {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.Sequence
}

func (i *Instance) getSuccessors() []node.NodeId {
	i.lock.RLock()
	defer i.lock.RUnlock()
	result := make([]node.NodeId, len(i.Successors))
	copy(result, i.Successors)
	return result
}

func (i *Instance) getDependencies() []InstanceID {
	// TODO: don't return a copy
	i.lock.RLock()
	defer i.lock.RUnlock()
	result := make([]InstanceID, len(i.Dependencies))
	copy(result, i.Dependencies)
	return result
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
		Sequence: i.Sequence,
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
func (i *Instance) mergeAttributes(seq uint64, deps []InstanceID) bool {
	i.lock.Lock()
	defer i.lock.Unlock()
	changes := false
	if seq > i.Sequence {
		changes = true
		i.Sequence = seq
	}
	iSet := NewInstanceIDSet(i.Dependencies)
	oSet := NewInstanceIDSet(deps)
	if !iSet.Equal(oSet) {
		changes = true
		union := iSet.Union(oSet)
		i.Dependencies = make([]InstanceID, 0, len(union))
		for id := range union {
			i.Dependencies = append(i.Dependencies, id)
		}
	}
	return changes
}

// -------------- state changes --------------

func (i *Instance) preaccept(inst *Instance, incrementBallot bool) error {
	var err error
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
	i.Sequence = i.manager.getNextSeq()
	if i.Dependencies, err = i.manager.getInstanceDeps(i); err != nil {
		return err
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
		i.Dependencies = inst.Dependencies

		i.Sequence = inst.Sequence
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
	}

	if inst != nil && inst != i {
		inst.lock.RLock()
		// this replica may have missed an accept message
		// so copy the seq & deps onto the existing instance
		i.Dependencies = inst.Dependencies

		i.Sequence = inst.Sequence
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

	// sequence
	numBytes += 8

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

	if err := binary.Write(buf, binary.LittleEndian, &i.Sequence); err != nil { return err }
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
	i.Dependencies = make([]InstanceID, numDeps)
	for idx := range i.Dependencies {
		if err := (&i.Dependencies[idx]).ReadBuffer(buf); err != nil { return err }
	}

	if err := binary.Read(buf, binary.LittleEndian, &i.Sequence); err != nil { return err }
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

