package consensus

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

import (
	"code.google.com/p/go-uuid/uuid"
)

import (
	"node"
	"serializer"
	"store"
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
		return fmt.Sprintf("Unknown InstanceStatus: %v", s)
	}
	panic("unreachable")
}

type InstanceID string

func NewInstanceID() InstanceID {
	return InstanceID(uuid.NewUUID())
}

func (i InstanceID) UUID() uuid.UUID {
	return uuid.UUID(i)
}

func (i InstanceID) String() string {
	return i.UUID().String()
}

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

// a serializable set of instructions
type Instance struct {
	// the uuid of this instance
	InstanceID InstanceID

	// the node id of the instance leader
	LeaderID node.NodeId

	// the order other nodes handle prepare phases
	Successors []node.NodeId

	// the Instructions(s) to be executed
	Commands []*store.Instruction

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

	// the instance's parent scope
	scope *Scope
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
	buf := &bytes.Buffer{}
	writer := bufio.NewWriter(buf)
	if err := instanceSerialize(i, writer); err != nil {
		return nil, err
	}
	if err := writer.Flush(); err != nil {
		return nil, err
	}
	reader := bufio.NewReader(buf)
	dst, err := instanceDeserialize(reader)
	if err != nil {
		return nil, err
	}
	return dst, nil
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
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.Status > INSTANCE_PREACCEPTED {
		return NewInvalidStatusUpdateError(i, INSTANCE_PREACCEPTED)
	}

	if inst != nil {
		i.Noop = inst.Noop
		if inst.MaxBallot > i.MaxBallot {
			i.MaxBallot = inst.MaxBallot
		}
	}
	i.Status = INSTANCE_PREACCEPTED
	i.Sequence, i.Dependencies = i.scope.getSeqAndDeps()
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

	if inst != nil {
		i.Dependencies = inst.Dependencies
		i.Sequence = inst.Sequence
		i.Noop = inst.Noop
		if inst.MaxBallot > i.MaxBallot {
			i.MaxBallot = inst.MaxBallot
		}
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

	if inst != nil {
		// this replica may have missed an accept message
		// so copy the seq & deps onto the existing instance
		i.Dependencies = inst.Dependencies
		i.Sequence = inst.Sequence
		i.Noop = inst.Noop
		if inst.MaxBallot > i.MaxBallot {
			i.MaxBallot = inst.MaxBallot
		}
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

func instructionSerialize(instruction *store.Instruction, buf *bufio.Writer) error {
	if err := serializer.WriteFieldString(buf, instruction.Cmd); err != nil { return err }
	if err := serializer.WriteFieldString(buf, instruction.Key); err != nil { return err }
	numArgs := uint32(len(instruction.Args))
	if err := binary.Write(buf, binary.LittleEndian, &numArgs); err != nil { return err }
	for _, arg := range instruction.Args {
		if err := serializer.WriteFieldString(buf, arg); err != nil { return err }
	}
	if err := serializer.WriteTime(buf, instruction.Timestamp); err != nil { return err }
	return nil
}

func instructionDeserialize(buf *bufio.Reader) (*store.Instruction, error) {
	instruction := &store.Instruction{}
	if val, err := serializer.ReadFieldString(buf); err != nil { return nil, err } else {
		instruction.Cmd = val
	}
	if val, err := serializer.ReadFieldString(buf); err != nil { return nil, err } else {
		instruction.Key = val
	}

	var numArgs uint32
	if err := binary.Read(buf, binary.LittleEndian, &numArgs); err != nil { return nil, err }
	instruction.Args = make([]string, numArgs)
	for i := range instruction.Args {
		if val, err := serializer.ReadFieldString(buf); err != nil { return nil, err } else {
			instruction.Args[i] = val
		}
	}
	if val, err := serializer.ReadTime(buf); err != nil { return nil, err } else {
		instruction.Timestamp = val
	}
	return instruction, nil
}

func instanceLimitedSerialize(instance *Instance, buf *bufio.Writer) error {
	if err := serializer.WriteFieldString(buf, string(instance.InstanceID)); err != nil { return err }
	if err := serializer.WriteFieldString(buf, string(instance.LeaderID)); err != nil { return err }
	numSuccessors := uint32(len(instance.Successors))
	if err := binary.Write(buf, binary.LittleEndian, &numSuccessors); err != nil { return err }
	for _, nid := range instance.Successors {
		if err := serializer.WriteFieldString(buf, string(nid)); err != nil { return err }
	}
	numInstructions := uint32(len(instance.Commands))
	if err := binary.Write(buf, binary.LittleEndian, &numInstructions); err != nil { return err }
	for _, inst := range instance.Commands {
		if err := instructionSerialize(inst, buf); err != nil { return err }
	}
	numDeps := uint32(len(instance.Dependencies))
	if err := binary.Write(buf, binary.LittleEndian, &numDeps); err != nil { return err }
	for _, dep := range instance.Dependencies {
		if err := serializer.WriteFieldString(buf, string(dep)); err != nil { return err }
	}

	if err := binary.Write(buf, binary.LittleEndian, &instance.Sequence); err != nil { return err }
	if err := binary.Write(buf, binary.LittleEndian, &instance.Status); err != nil { return err }
	if err := binary.Write(buf, binary.LittleEndian, &instance.MaxBallot); err != nil { return err }

	var noop byte
	if instance.Noop { noop = 0xff }
	if err := binary.Write(buf, binary.LittleEndian, &noop); err != nil { return err }

	var match byte
	if instance.DependencyMatch { match = 0xff }
	if err := binary.Write(buf, binary.LittleEndian, &match); err != nil { return err }

	return nil
}

func instanceLimitedDeserialize(buf *bufio.Reader) (*Instance, error) {
	instance := &Instance{}
	if val, err := serializer.ReadFieldString(buf); err != nil { return nil, err } else {
		instance.InstanceID = InstanceID(val)
	}
	if val, err := serializer.ReadFieldString(buf); err != nil { return nil, err } else {
		instance.LeaderID = node.NodeId(val)
	}

	var numSuccessors uint32
	if err := binary.Read(buf, binary.LittleEndian, &numSuccessors); err != nil { return nil, err }
	instance.Successors = make([]node.NodeId, numSuccessors)
	for i := range instance.Successors {
		if val, err := serializer.ReadFieldString(buf); err != nil { return nil, err } else {
			instance.Successors[i] = node.NodeId(val)
		}
	}

	var numInstructions uint32
	if err := binary.Read(buf, binary.LittleEndian, &numInstructions); err != nil { return nil, err }
	instance.Commands = make([]*store.Instruction, numInstructions)
	for i := range instance.Commands {
		instr, err := instructionDeserialize(buf)
		if err != nil { return nil, err }
		instance.Commands[i] = instr
	}

	var numDeps uint32
	if err := binary.Read(buf, binary.LittleEndian, &numDeps); err != nil { return nil, err }
	instance.Dependencies = make([]InstanceID, numDeps)
	for i := range instance.Dependencies {
		if dep, err := serializer.ReadFieldString(buf); err != nil { return nil, err } else {
			instance.Dependencies[i] = InstanceID(dep)
		}
	}

	if err := binary.Read(buf, binary.LittleEndian, &instance.Sequence); err != nil { return nil, err }
	if err := binary.Read(buf, binary.LittleEndian, &instance.Status); err != nil { return nil, err }
	if err := binary.Read(buf, binary.LittleEndian, &instance.MaxBallot); err != nil { return nil, err }

	var noop byte
	if err := binary.Read(buf, binary.LittleEndian, &noop); err != nil { return nil, err }
	instance.Noop = noop != 0x0

	var match byte
	if err := binary.Read(buf, binary.LittleEndian, &match); err != nil { return nil, err }
	instance.DependencyMatch = match != 0x0

	return instance, nil
}

func instanceSerialize(instance *Instance, buf *bufio.Writer) error {
	if err := instanceLimitedSerialize(instance, buf); err != nil { return err }
	if err := serializer.WriteTime(buf, instance.commitTimeout); err != nil { return err }
	if err := serializer.WriteTime(buf, instance.executeTimeout); err != nil { return err }

	return nil
}

func instanceDeserialize(buf *bufio.Reader) (*Instance, error) {
	var instance *Instance
	if val, err := instanceLimitedDeserialize(buf); err != nil { return nil, err } else {
		instance = val
	}
	if val, err := serializer.ReadTime(buf); err != nil { return nil, err } else {
		instance.commitTimeout = val
	}
	if val, err := serializer.ReadTime(buf); err != nil { return nil, err } else {
		instance.executeTimeout = val
	}

	return instance, nil
}
