package cluster

import (
	"store"
	"time"
)

type reconcileCall struct {
	key string
	values map[string] store.Value
}

type reconcileResponse struct {
	val store.Value
	instructions map[string][]*store.Instruction
	err error
}

type mockStore struct {
	readInstructions []*readCall
	readResponses []*mockQueryResponse
	writeInstructions []*writeCall
	writeResponses []*mockQueryResponse
	reconcileCalls []*reconcileCall
	reconcileResponses []*reconcileResponse
	//
	isStarted bool

	// blanket responses to is read/write
	isRead bool
	isWrite bool
}

func newMockStore() *mockStore {
	s := &mockStore{}
	s.isRead = true
	s.isWrite = true
	s.readInstructions = make([]*readCall, 0, 10)
	s.readResponses = make([]*mockQueryResponse, 0, 10)
	s.writeInstructions = make([]*writeCall, 0, 10)
	s.writeResponses = make([]*mockQueryResponse, 0, 10)
	s.reconcileCalls = make([]*reconcileCall, 0, 10)
	s.reconcileResponses = make([]*reconcileResponse, 0, 10)

	return s
}

func (s *mockStore) addReadResponse(val store.Value, err error) {
	s.readResponses = append(s.readResponses, &mockQueryResponse{val:val, err:err})
}

func (s *mockStore) addWriteResponse(val store.Value, err error) {
	s.writeResponses = append(s.writeResponses, &mockQueryResponse{val:val, err:err})
}

func (s *mockStore) addReconcileResponse(val store.Value, instructions map[string][]*store.Instruction, err error) {
	s.reconcileResponses = append(s.reconcileResponses, &reconcileResponse{val:val, instructions:instructions, err:err})
}

func (s *mockStore) ExecuteRead(cmd string, key string, args []string) (store.Value, error) {
	rc := &readCall{cmd:cmd, key:key, args:args}
	s.readInstructions = append(s.readInstructions, rc)
	rr := s.readResponses[0]
	s.readResponses = s.readResponses[1:]
	return rr.val, rr.err
}

// executes a write instruction against the node's store
func (s *mockStore) ExecuteWrite(cmd string, key string, args []string, timestamp time.Time) (store.Value, error) {
	rc := &writeCall{cmd:cmd, key:key, args:args, timestamp:timestamp}
	s.writeInstructions = append(s.writeInstructions, rc)
	rr := s.writeResponses[0]
	s.writeResponses = s.writeResponses[1:]
	return rr.val, rr.err
}

func (s *mockStore) Reconcile(key string, values map[string] store.Value) (store.Value, map[string][]*store.Instruction, error) {
	rc := &reconcileCall{key:key, values:values}
	s.reconcileCalls = append(s.reconcileCalls, rc)
	rr := s.reconcileResponses[0]
	s.reconcileResponses = s.reconcileResponses[1:]
	return rr.val, rr.instructions, rr.err
}

func (s *mockStore) IsReadCommand(cmd string) bool { return s.isRead }
func (s *mockStore) IsWriteCommand(cmd string) bool { return s.isWrite }
func (s *mockStore) Start() error { s.isStarted = true; return nil }
func (s *mockStore) Stop() error { s.isStarted = true; return nil }
func (s *mockStore) SerializeValue(v store.Value) ([]byte, error) { return []byte{}, nil }
func (s *mockStore) DeserializeValue(b []byte) (store.Value, store.ValueType, error) { return nil, store.ValueType(0), nil }
func (s *mockStore) GetRawKey(key string) (store.Value, error) { return nil, nil}
func (s *mockStore) SetRawKey(key string, val store.Value) error { return nil }
func (s *mockStore) GetKeys() []string { return []string{} }
func (s *mockStore) KeyExists(key string) bool { return true }
