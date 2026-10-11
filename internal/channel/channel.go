package channel

import "errors"

// Channel sends and receives datagrams. A datagram is an opaque
// serialized packet. The channel does not interpret its bytes.
type Channel interface {
	// Send queues a datagram for delivery. The caller keeps ownership of data.
	Send(data []byte) error

	// Receive returns the next datagram delivered to this channel.
	// It returns ErrEmpty when nothing is waiting.
	Receive() ([]byte, error)
}

// ErrEmpty is returned when Receive is called and no datagram is waiting.
var ErrEmpty = errors.New("no datagram waiting")
