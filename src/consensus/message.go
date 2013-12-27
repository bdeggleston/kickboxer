package consensus

type ScopedMessage interface {
	GetScope() string
}

type PreAcceptRequest struct {
	// the scope name the message
	// is going to
	Scope    string
	Instance *Instance
}

type PreAcceptResponse struct {
	Accepted bool
	Instance *Instance
}
