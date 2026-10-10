package protocol

import (
	"bytes"
	"errors"
	"testing"
)

func TestSenderInitialState(t *testing.T) {
	s := NewSender()
	if s.State() != WaitForCall0 {
		t.Fatalf("state = %s, want %s", s.State(), WaitForCall0)
	}
	if s.Packet() != nil {
		t.Fatal("initial packet is not nil")
	}
	assertAccepted(t, s.State().AcceptedEvents(), CallFromAbove)
	if s.State().Accepts(PacketFromBelow) {
		t.Fatal("idle sender accepts a packet from below")
	}
}

func TestSenderEveryTransition(t *testing.T) {
	s := NewSender()

	first := []byte("seq-0")
	actions := mustHandle(t, s, Event{Kind: CallFromAbove, Data: first})
	pkt0 := onePacket(t, actions, SendPacket)
	if pkt0.Type != Data || pkt0.Seq != 0 || !bytes.Equal(pkt0.Payload, first) {
		t.Fatalf("packet = {Type:%d Seq:%d Payload:%q}", pkt0.Type, pkt0.Seq, pkt0.Payload)
	}
	if pkt0 != s.Packet() {
		t.Fatal("send did not keep the packet")
	}
	if s.State() != WaitForACKOrNAK0 {
		t.Fatalf("state = %s, want %s", s.State(), WaitForACKOrNAK0)
	}
	assertAccepted(t, s.State().AcceptedEvents(), PacketFromBelow)

	actions = mustHandle(t, s, Event{Kind: PacketFromBelow, Corrupt: true, Packet: &Packet{Type: ACK}})
	if got := onePacket(t, actions, Retransmit); got != pkt0 {
		t.Fatal("corrupt ACK was not a retransmit of the stored packet")
	}
	assertSenderHeld(t, s, WaitForACKOrNAK0, pkt0)

	actions = mustHandle(t, s, Event{Kind: PacketFromBelow, Packet: &Packet{Type: NAK, Seq: 9, Payload: []byte("nak")}})
	if got := onePacket(t, actions, Retransmit); got != pkt0 {
		t.Fatal("NAK was not a retransmit of the stored packet")
	}
	assertSenderHeld(t, s, WaitForACKOrNAK0, pkt0)

	actions = mustHandle(t, s, Event{Kind: PacketFromBelow, Packet: &Packet{Type: ACK, Seq: 99, Payload: []byte("ignored")}})
	if len(actions) != 0 {
		t.Fatalf("ACK actions = %+v, want none", actions)
	}
	if s.State() != WaitForCall1 {
		t.Fatalf("state = %s, want %s", s.State(), WaitForCall1)
	}
	if s.Packet() != nil {
		t.Fatal("packet kept after ACK")
	}

	second := []byte("seq-1")
	actions = mustHandle(t, s, Event{Kind: CallFromAbove, Data: second})
	pkt1 := onePacket(t, actions, SendPacket)
	if pkt1.Type != Data || pkt1.Seq != 1 || !bytes.Equal(pkt1.Payload, second) {
		t.Fatalf("packet = {Type:%d Seq:%d Payload:%q}", pkt1.Type, pkt1.Seq, pkt1.Payload)
	}
	if pkt1 == pkt0 {
		t.Fatal("sequence 1 reused the sequence 0 packet")
	}
	assertSenderHeld(t, s, WaitForACKOrNAK1, pkt1)

	actions = mustHandle(t, s, Event{Kind: PacketFromBelow, Corrupt: true})
	if got := onePacket(t, actions, Retransmit); got != pkt1 {
		t.Fatal("corrupt arrival was not a retransmit of the stored packet")
	}
	assertSenderHeld(t, s, WaitForACKOrNAK1, pkt1)

	actions = mustHandle(t, s, Event{Kind: PacketFromBelow, Packet: &Packet{Type: NAK}})
	if got := onePacket(t, actions, Retransmit); got != pkt1 {
		t.Fatal("NAK was not a retransmit of the stored packet")
	}
	assertSenderHeld(t, s, WaitForACKOrNAK1, pkt1)

	actions = mustHandle(t, s, Event{Kind: PacketFromBelow, Packet: &Packet{Type: ACK}})
	if len(actions) != 0 {
		t.Fatalf("ACK actions = %+v, want none", actions)
	}
	if s.State() != WaitForCall0 {
		t.Fatalf("state = %s, want %s", s.State(), WaitForCall0)
	}
	if s.Packet() != nil {
		t.Fatal("packet kept after ACK")
	}
}

func TestSenderRejectsUnacceptedEvents(t *testing.T) {
	s := NewSender()
	mustReject(t, s, Event{Kind: PacketFromBelow, Packet: &Packet{Type: ACK}}, ErrEventNotAccepted)
	mustReject(t, s, Event{}, ErrUnknownEvent)
	assertSenderHeld(t, s, WaitForCall0, nil)

	data := []byte("kept")
	pkt := onePacket(t, mustHandle(t, s, Event{Kind: CallFromAbove, Data: data}), SendPacket)

	mustReject(t, s, Event{Kind: CallFromAbove, Data: []byte("blocked")}, ErrEventNotAccepted)
	mustReject(t, s, Event{Kind: PacketFromBelow}, ErrMissingPacket)
	mustReject(t, s, Event{Kind: PacketFromBelow, Packet: &Packet{Type: Data, Seq: 0, Payload: data}}, ErrNoTransition)
	assertSenderHeld(t, s, WaitForACKOrNAK0, pkt)

	s.state = 99
	mustReject(t, s, Event{Kind: CallFromAbove, Data: []byte{1}}, ErrEventNotAccepted)
}

func TestSenderPayloadIsolation(t *testing.T) {
	s := NewSender()
	data := []byte("hello")
	pkt := onePacket(t, mustHandle(t, s, Event{Kind: CallFromAbove, Data: data}), SendPacket)

	data[0] = 'X'
	if pkt.Payload[0] != 'h' {
		t.Fatal("sender aliased the caller's data")
	}
	if !bytes.Equal(s.Packet().Payload, []byte("hello")) {
		t.Fatalf("stored payload = %q", s.Packet().Payload)
	}
}

func TestSenderRejectsOversizedPayload(t *testing.T) {
	s := NewSender()
	_, err := s.Handle(Event{Kind: CallFromAbove, Data: make([]byte, 0xFFFF+1)})
	if err == nil {
		t.Fatal("Handle() error = nil, want payload length error")
	}
	if err.Error() != "payload exceeds maximum length" {
		t.Fatalf("Handle() error = %q", err.Error())
	}
	assertSenderHeld(t, s, WaitForCall0, nil)
}

func TestSenderEmptyPayload(t *testing.T) {
	s := NewSender()
	pkt := onePacket(t, mustHandle(t, s, Event{Kind: CallFromAbove}), SendPacket)
	if pkt.Type != Data || pkt.Seq != 0 || len(pkt.Payload) != 0 {
		t.Fatalf("packet = {Type:%d Seq:%d Payload:%x}", pkt.Type, pkt.Seq, pkt.Payload)
	}
	assertSenderHeld(t, s, WaitForACKOrNAK0, pkt)
}

func onePacket(t *testing.T, actions []Action, kind ActionKind) *Packet {
	t.Helper()
	if len(actions) != 1 {
		t.Fatalf("actions = %d, want 1", len(actions))
	}
	return assertSend(t, actions[0], kind)
}

func assertAccepted(t *testing.T, got []EventKind, want EventKind) {
	t.Helper()
	if len(got) != 1 || got[0] != want {
		t.Fatalf("accepted events = %v, want [%s]", got, want)
	}
}

func TestSenderErrorIsWrapped(t *testing.T) {
	s := NewSender()
	_, err := s.Handle(Event{Kind: PacketFromBelow, Packet: &Packet{Type: ACK}})
	if !errors.Is(err, ErrEventNotAccepted) {
		t.Fatalf("Handle() error = %v", err)
	}
}
