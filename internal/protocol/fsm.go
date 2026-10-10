package protocol

import (
	"bytes"
	"errors"
	"fmt"
)

// EventKind is an input a protocol state can accept.
type EventKind int

const (
	// CallFromAbove is rdt_send(data).
	CallFromAbove EventKind = iota + 1
	// PacketFromBelow is rdt_rcv(rcvpkt).
	PacketFromBelow
)

func (k EventKind) String() string {
	switch k {
	case CallFromAbove:
		return "call from above"
	case PacketFromBelow:
		return "packet from below"
	default:
		return "unknown event"
	}
}

// Event is one input to an rdt 2.1 state machine.
//
// CallFromAbove carries Data. PacketFromBelow carries Packet.
// Corrupt reports a failed checksum. The machine ignores Packet in that
// case, so checksum checks stay outside the state machine.
type Event struct {
	Kind    EventKind
	Data    []byte
	Packet  *Packet
	Corrupt bool
}

// ActionKind is work the caller performs after a transition.
type ActionKind int

const (
	// SendPacket sends a packet this transition just built.
	SendPacket ActionKind = iota + 1
	// Retransmit sends the data packet kept since the last send.
	Retransmit
	// DeliverData passes payload up to the application.
	DeliverData
)

func (k ActionKind) String() string {
	switch k {
	case SendPacket:
		return "send packet"
	case Retransmit:
		return "retransmit"
	case DeliverData:
		return "deliver data"
	default:
		return "unknown action"
	}
}

// Action is one output of a transition. A nil slice is the figure's Λ:
// the transition accepts the event and performs no action.
type Action struct {
	Kind   ActionKind
	Packet *Packet
	Data   []byte
}

var (
	ErrUnknownEvent     = errors.New("unknown protocol event")
	ErrEventNotAccepted = errors.New("event not accepted in this state")
	ErrMissingPacket    = errors.New("packet from below is missing")
	ErrNoTransition     = errors.New("packet matches no transition")
)

func knownEvent(kind EventKind) bool {
	switch kind {
	case CallFromAbove, PacketFromBelow:
		return true
	default:
		return false
	}
}

func accepts(events []EventKind, kind EventKind) bool {
	for _, event := range events {
		if event == kind {
			return true
		}
	}
	return false
}

func stateError(state fmt.Stringer, err error) ([]Action, error) {
	return nil, fmt.Errorf("%s: %w", state, err)
}

// makePacket builds a packet and fills its checksum.
// Sending the serialized bytes is left to the caller.
func makePacket(typ PacketType, seq uint8, payload []byte) (*Packet, error) {
	pkt := &Packet{
		Type:    typ,
		Seq:     seq,
		Payload: bytes.Clone(payload),
	}
	if _, err := pkt.Serialize(); err != nil {
		return nil, err
	}
	return pkt, nil
}
