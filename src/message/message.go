package message

import (
	"bufio"
)

// message wire protocol is as follow:
// [type (4b)][num fields (4b)]...[field size (4b)][field data]
// each message type can define the data
// format as needed
type Message interface {

	// serializes the entire message, including
	// headers. Pass a wrapped connection in to send
	Serialize(*bufio.Writer) error

	// deserializes everything after the size data
	// pass in a wrapped exception to receive
	Deserialize(*bufio.Reader) error

	// returns the message type enum
	GetType() uint32
}
