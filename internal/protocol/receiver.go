package protocol

import "bytes"

// ReceiverState is one state of the rdt 2.1 receiver.
type ReceiverState int

const (
	// WaitFor0FromBelow delivers a not-corrupt sequence 0 packet, then
	// sends ACK. A corrupt packet is answered with NAK. A not-corrupt
	// sequence 1 packet is a duplicate and is answered with ACK.
	WaitFor0FromBelow ReceiverState = iota
	// `WaitFor1FromBelow` delivers a not-corrupt sequence 1 packet, then
	// sends ACK. A corrupt packet is answered with NAK. A not-corrupt
	// sequence 0 packet is a duplicate and is answered with ACK.
	WaitFor1FromBelow
)

func (s ReceiverState) String() string {
	switch s {
	case WaitFor0FromBelow:
		return "wait for 0 from below"
	case WaitFor1FromBelow:
		return "wait for 1 from below"
	default:
		return "unknown receiver state"
	}
}

// AcceptedEvents lists the events this state handles.
func (s ReceiverState) AcceptedEvents() []EventKind {
	switch s {
	case WaitFor0FromBelow, WaitFor1FromBelow:
		return []EventKind{PacketFromBelow}
	default:
		return nil
	}
}

// Accepts reports whether kind is handled in this state.
func (s ReceiverState) Accepts(kind EventKind) bool {
	return accepts(s.AcceptedEvents(), kind)
}

// Receiver is the rdt 2.1 receiver. Handle returns the actions for one
// transition and does not read or write the network.
type Receiver struct {
	state ReceiverState
}

func NewReceiver() *Receiver {
	return &Receiver{state: WaitFor0FromBelow}
}

func (r *Receiver) State() ReceiverState {
	return r.state
}

// Handle applies one packet from below. A packet in the expected
// sequence returns DeliverData and then SendPacket(ACK).
func (r *Receiver) Handle(event Event) ([]Action, error) {
	if !knownEvent(event.Kind) {
		return stateError(r.state, ErrUnknownEvent)
	}
	if !r.state.Accepts(event.Kind) {
		return stateError(r.state, ErrEventNotAccepted)
	}

	switch r.state {
	case WaitFor0FromBelow:
		return r.receive(event, 0, WaitFor1FromBelow)
	case WaitFor1FromBelow:
		return r.receive(event, 1, WaitFor0FromBelow)
	default:
		return stateError(r.state, ErrEventNotAccepted)
	}
}

// receive applies the three rdt_rcv edges of one receiver state.
// expected is delivered. The other sequence number is acknowledged
// again and not delivered.
func (r *Receiver) receive(event Event, expected uint8, next ReceiverState) ([]Action, error) {
	if event.Corrupt {
		return control(NAK)
	}
	if event.Packet == nil {
		return stateError(r.state, ErrMissingPacket)
	}

	pkt := event.Packet
	if pkt.Type != Data {
		return stateError(r.state, ErrNoTransition)
	}

	// Sequence numbers are one bit, so the other value is a duplicate.
	duplicate := expected ^ 1
	switch pkt.Seq {
	case expected:
		ack, err := makePacket(ACK, 0, nil)
		if err != nil {
			return nil, err
		}
		r.state = next
		return []Action{
			{Kind: DeliverData, Data: bytes.Clone(pkt.Payload)},
			{Kind: SendPacket, Packet: ack},
		}, nil
	case duplicate:
		return control(ACK)
	default:
		return stateError(r.state, ErrNoTransition)
	}
}

func control(typ PacketType) ([]Action, error) {
	pkt, err := makePacket(typ, 0, nil)
	if err != nil {
		return nil, err
	}
	return []Action{{Kind: SendPacket, Packet: pkt}}, nil
}
