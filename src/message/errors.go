package message

type MessageError struct {
	reason string
}

func (e *MessageError) Error() string {
	return e.reason
}

type MessageEncodingError struct {
	MessageError
}

func NewMessageEncodingError(reason string) *MessageEncodingError {
	return &MessageEncodingError{MessageError{reason}}
}
