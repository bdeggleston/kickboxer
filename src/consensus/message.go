package consensus

import (
	"bufio"
)

import (
	"message"
)

type ScopedMessage interface {
	message.Message
	GetScope() string
}

// cheats the message interface implementation
// TODO: actually implement the message interface
type messageCheat struct { }
func (m *messageCheat) Serialize(*bufio.Writer) error { return nil }
func (m *messageCheat) Deserialize(*bufio.Reader) error { return nil }
func (m *messageCheat) GetType() uint32 { return 0 }

type PreAcceptRequest struct {
	messageCheat

	// the scope name the message
	// is going to
	Scope    string

	Instance *Instance
}

type PreAcceptResponse struct {
	messageCheat

	// indicates the remote node ignored the
	// preaccept due to an out of date ballot
	Accepted bool

	// the instance info the remote node added
	// to it's instances. Sequence and Dependencies
	// may be different
	Instance *Instance
}
