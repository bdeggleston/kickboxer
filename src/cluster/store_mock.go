package cluster

import (
	"bufio"
	"store"
	"time"
)

import (
	"serializer"
)

type reconcileCall struct {
	key string
	values []store.Value
}

type reconcileResponse struct {
	val store.Value
	instructions [][]store.Instruction
	err error
}

type mockStore struct {
	requests []*queryCall
	responses []*mockQueryResponse
	reconcileCalls []*reconcileCall
	reconcileResponses []*reconcileResponse
	//
	isStarted bool

	// blanket responses to is read/write
	isRead bool
	isWrite bool
	returnsValue bool
}

var _ = store.Store(&mockStore{})

func newMockStore() *mockStore {
	s := &mockStore{}
	s.isRead = true
	s.isWrite = true
	s.returnsValue = true
	s.requests = make([]*queryCall, 0, 10)
	s.responses = make([]*mockQueryResponse, 0, 10)
	s.reconcileCalls = make([]*reconcileCall, 0, 10)
	s.reconcileResponses = make([]*reconcileResponse, 0, 10)

	return s
}

func (s *mockStore) addResponse(val store.Value, err error) {
	s.responses = append(s.responses, &mockQueryResponse{val:val, err:err})
}

func (s *mockStore) addReconcileResponse(val store.Value, instructions [][]store.Instruction, err error) {
	s.reconcileResponses = append(s.reconcileResponses, &reconcileResponse{val:val, instructions:instructions, err:err})
}

// executes a write instruction against the node's store
func (s *mockStore) ExecuteInstruction(instruction store.Instruction) (store.Value, error) {
	rc := &queryCall{
		cmd:instruction.Cmd,
		key:instruction.Key,
		args:instruction.Args,
		timestamp:instruction.Timestamp,
	}
	s.requests = append(s.requests, rc)
	rr := s.responses[0]
	s.responses = s.responses[1:]
	return rr.val, rr.err
}

func (s *mockStore) Reconcile(key string, values []store.Value) (store.Value, [][]store.Instruction, error) {
	rc := &reconcileCall{key:key, values:values}
	s.reconcileCalls = append(s.reconcileCalls, rc)
	rr := s.reconcileResponses[0]
	s.reconcileResponses = s.reconcileResponses[1:]
	return rr.val, rr.instructions, rr.err
}

func (s *mockStore) IsReadOnly(instruction store.Instruction) bool { return s.isRead }
func (s *mockStore) IsWriteOnly(instruction store.Instruction) bool { return s.isWrite }
func (s *mockStore) InterferingKeys(instruction store.Instruction) []string { return []string{instruction.Key} }
func (s *mockStore) ReturnsValue(cmd string) bool { return s.returnsValue }
func (s *mockStore) Start() error { s.isStarted = true; return nil }
func (s *mockStore) Stop() error { s.isStarted = true; return nil }
func (s *mockStore) SerializeValue(v store.Value) ([]byte, error) { return []byte{}, nil }
func (s *mockStore) DeserializeValue(b []byte) (store.Value, store.ValueType, error) { return nil, store.ValueType(0), nil }
func (s *mockStore) GetRawKey(key string) (store.Value, error) { return nil, nil}
func (s *mockStore) SetRawKey(key string, val store.Value) error { return nil }
func (s *mockStore) GetKeys() []string { return []string{} }
func (s *mockStore) KeyExists(key string) bool { return true }

// values

// a single value used for
// key/val types
type mockStringVal struct {
	value string
	time time.Time
}

// single value constructor
func newMockString(value string, time time.Time) *mockStringVal {
	v := &mockStringVal{
		value:value,
		time:time,
	}
	return v
}

func (v *mockStringVal) GetValue() string {
	return v.value
}

func (v *mockStringVal) GetTimestamp() time.Time {
	return v.time
}

func (v *mockStringVal) GetValueType() store.ValueType {
	return "STRING"
}
func (v *mockStringVal) Equal(o store.Value) bool {
	if !baseValueEqual(v, o) { return false }
	other := o.(*mockStringVal)
	if v.value != other.value { return false }
	return true
}

func (v *mockStringVal) Serialize(buf *bufio.Writer) error {
	if err := serializer.WriteFieldBytes(buf, []byte(v.value)); err != nil {
		return err
	}
	if err := serializer.WriteTime(buf, v.time); err != nil {
		return err
	}
	if err := buf.Flush(); err != nil {
		return err
	}
	return nil
}

func (v *mockStringVal) Deserialize(buf *bufio.Reader) error {
	if val, err := serializer.ReadFieldBytes(buf); err != nil {
		return err
	} else {
		v.value = string(val)
	}

	if t, err := serializer.ReadTime(buf); err != nil {
		return err
	} else {
		v.time = t
	}
	return nil
}

func baseValueEqual(v0, v1 store.Value) bool {
	if v0.GetValueType() != v1.GetValueType() { return false }
	if v0.GetTimestamp() != v1.GetTimestamp() { return false }
	return true
}

