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

type BallotMessage interface {
	message.Message
	GetBallot() uint32
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

func (m *PreAcceptRequest) GetScope() string { return m.Scope }

type PreAcceptResponse struct {
	messageCheat

	// indicates the remote node ignored the
	// preaccept due to an out of date ballot
	Accepted bool

	// the highest ballot that's been seen
	// for this instance
	MaxBallot uint32

	// the instance info the remote node added
	// to it's instances. Sequence and Dependencies
	// may be different
	Instance *Instance

	// if the command leader seems to be missing
	// instances in it's deps, return them here
	MissingInstances []*Instance
}

func (m *PreAcceptResponse) GetBallot() uint32 { return m.MaxBallot }

type AcceptRequest struct {
	messageCheat

	// the scope name the message
	// is going to
	Scope    string

	// the instance the remote node is instructed
	// to accept
	Instance *Instance

	// if the replica seems to be missing
	// instances in it's deps, send them here
	MissingInstances []*Instance
}

func (m *AcceptRequest) GetScope() string { return m.Scope }

type AcceptResponse struct {
	messageCheat

	// indicates the remote node ignored the
	// preaccept due to an out of date ballot
	Accepted bool

	// the highest ballot that's been seen
	// for this instance
	MaxBallot uint32
}

func (m *AcceptResponse) GetBallot() uint32 { return m.MaxBallot }

type CommitRequest struct {
	messageCheat

	// the scope name the message
	// is going to
	Scope    string

	// the id of the messages to be committed
	InstanceID InstanceID
}

func (m *CommitRequest) GetScope() string { return m.Scope }

type CommitResponse struct {
	messageCheat
}
