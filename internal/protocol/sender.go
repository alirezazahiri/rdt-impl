package protocol

// SenderState is one state of the rdt 2.1 sender.
type SenderState int

const (
	// WaitForCall0 accepts a call from above, sends sequence 0, and
	// keeps that packet until it is acknowledged.
	WaitForCall0 SenderState = iota
	// WaitForACKOrNAK0 retransmits the stored packet when the arrival
	// is corrupt or a NAK. A not-corrupt ACK moves to WaitForCall1.
	WaitForACKOrNAK0
	// WaitForCall1 accepts a call from above, sends sequence 1, and
	// keeps that packet until it is acknowledged.
	WaitForCall1
	// WaitForACKOrNAK1 retransmits the stored packet when the arrival
	// is corrupt or a NAK. A not-corrupt ACK moves to WaitForCall0.
	WaitForACKOrNAK1
)

func (s SenderState) String() string {
	switch s {
	case WaitForCall0:
		return "wait for call 0 from above"
	case WaitForACKOrNAK0:
		return "wait for ACK or NAK 0"
	case WaitForCall1:
		return "wait for call 1 from above"
	case WaitForACKOrNAK1:
		return "wait for ACK or NAK 1"
	default:
		return "unknown sender state"
	}
}

// AcceptedEvents lists the events this state handles.
func (s SenderState) AcceptedEvents() []EventKind {
	switch s {
	case WaitForCall0, WaitForCall1:
		return []EventKind{CallFromAbove}
	case WaitForACKOrNAK0, WaitForACKOrNAK1:
		return []EventKind{PacketFromBelow}
	default:
		return nil
	}
}

// Accepts reports whether kind is handled in this state.
func (s SenderState) Accepts(kind EventKind) bool {
	return accepts(s.AcceptedEvents(), kind)
}

// Sender is the rdt 2.1 sender. Handle returns the actions for one
// transition and does not read or write the network.
type Sender struct {
	state  SenderState
	sndpkt *Packet
}

func NewSender() *Sender {
	return &Sender{state: WaitForCall0}
}

func (s *Sender) State() SenderState {
	return s.state
}

// Packet is the data packet kept until a not-corrupt ACK arrives.
// It is nil while the sender is waiting for a call from above.
// Callers must not modify the returned packet.
func (s *Sender) Packet() *Packet {
	return s.sndpkt
}

// Handle applies one event. SendPacket and Retransmit carry the stored
// packet. A not-corrupt ACK returns no actions.
func (s *Sender) Handle(event Event) ([]Action, error) {
	if !knownEvent(event.Kind) {
		return stateError(s.state, ErrUnknownEvent)
	}
	if !s.state.Accepts(event.Kind) {
		return stateError(s.state, ErrEventNotAccepted)
	}

	switch s.state {
	case WaitForCall0:
		return s.send(event.Data, 0, WaitForACKOrNAK0)
	case WaitForCall1:
		return s.send(event.Data, 1, WaitForACKOrNAK1)
	case WaitForACKOrNAK0:
		return s.feedback(event, WaitForCall1)
	case WaitForACKOrNAK1:
		return s.feedback(event, WaitForCall0)
	default:
		return stateError(s.state, ErrEventNotAccepted)
	}
}

func (s *Sender) send(data []byte, seq uint8, next SenderState) ([]Action, error) {
	pkt, err := makePacket(Data, seq, data)
	if err != nil {
		return nil, err
	}
	s.sndpkt = pkt
	s.state = next
	return []Action{{Kind: SendPacket, Packet: pkt}}, nil
}

// feedback is the wait-for-ACK-or-NAK transition.
// corrupt(rcvpkt) || isNAK(rcvpkt) retransmits sndpkt.
// notcorrupt(rcvpkt) && isACK(rcvpkt) takes the Λ edge.
func (s *Sender) feedback(event Event, next SenderState) ([]Action, error) {
	if event.Corrupt || isNAK(event.Packet) {
		return []Action{{Kind: Retransmit, Packet: s.sndpkt}}, nil
	}
	if event.Packet == nil {
		return stateError(s.state, ErrMissingPacket)
	}
	if isACK(event.Packet) {
		s.sndpkt = nil
		s.state = next
		return nil, nil
	}
	return stateError(s.state, ErrNoTransition)
}

func isACK(pkt *Packet) bool {
	return pkt != nil && pkt.Type == ACK
}

func isNAK(pkt *Packet) bool {
	return pkt != nil && pkt.Type == NAK
}
