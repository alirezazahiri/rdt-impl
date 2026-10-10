package protocol

import (
	"bytes"
	"testing"
)

func TestReceiverInitialState(t *testing.T) {
	r := NewReceiver()
	if r.State() != WaitFor0FromBelow {
		t.Fatalf("state = %s, want %s", r.State(), WaitFor0FromBelow)
	}
	assertAccepted(t, r.State().AcceptedEvents(), PacketFromBelow)
	if r.State().Accepts(CallFromAbove) {
		t.Fatal("receiver accepts a call from above")
	}
}

func TestReceiverEveryTransition(t *testing.T) {
	r := NewReceiver()

	actions := mustHandle(t, r, Event{
		Kind:    PacketFromBelow,
		Corrupt: true,
		Packet:  &Packet{Type: Data, Seq: 0, Payload: []byte("hidden")},
	})
	nak := onePacket(t, actions, SendPacket)
	if nak.Type != NAK || nak.Seq != 0 || len(nak.Payload) != 0 {
		t.Fatalf("corrupt reply = {Type:%d Seq:%d Payload:%x}", nak.Type, nak.Seq, nak.Payload)
	}
	assertReceiverHeld(t, r, WaitFor0FromBelow)

	actions = mustHandle(t, r, Event{Kind: PacketFromBelow, Packet: dataPacket(1, []byte("old"))})
	ack := onePacket(t, actions, SendPacket)
	if ack.Type != ACK || ack.Seq != 0 || len(ack.Payload) != 0 {
		t.Fatalf("duplicate reply = {Type:%d Seq:%d Payload:%x}", ack.Type, ack.Seq, ack.Payload)
	}
	assertReceiverHeld(t, r, WaitFor0FromBelow)

	payload := []byte("first")
	actions = mustHandle(t, r, Event{Kind: PacketFromBelow, Packet: dataPacket(0, payload)})
	assertDelivered(t, actions, payload, ACK)
	if r.State() != WaitFor1FromBelow {
		t.Fatalf("state = %s, want %s", r.State(), WaitFor1FromBelow)
	}
	assertAccepted(t, r.State().AcceptedEvents(), PacketFromBelow)

	actions = mustHandle(t, r, Event{Kind: PacketFromBelow, Corrupt: true})
	nak = onePacket(t, actions, SendPacket)
	if nak.Type != NAK {
		t.Fatalf("corrupt reply type = %d, want NAK", nak.Type)
	}
	assertReceiverHeld(t, r, WaitFor1FromBelow)

	actions = mustHandle(t, r, Event{Kind: PacketFromBelow, Packet: dataPacket(0, []byte("first"))})
	ack = onePacket(t, actions, SendPacket)
	if ack.Type != ACK {
		t.Fatalf("duplicate reply type = %d, want ACK", ack.Type)
	}
	assertReceiverHeld(t, r, WaitFor1FromBelow)

	actions = mustHandle(t, r, Event{Kind: PacketFromBelow, Packet: dataPacket(1, []byte("second"))})
	assertDelivered(t, actions, []byte("second"), ACK)
	if r.State() != WaitFor0FromBelow {
		t.Fatalf("state = %s, want %s", r.State(), WaitFor0FromBelow)
	}
}

func TestReceiverRejectsUnacceptedEvents(t *testing.T) {
	r := NewReceiver()
	mustReject(t, r, Event{Kind: CallFromAbove, Data: []byte("no")}, ErrEventNotAccepted)
	mustReject(t, r, Event{}, ErrUnknownEvent)
	mustReject(t, r, Event{Kind: PacketFromBelow}, ErrMissingPacket)
	mustReject(t, r, Event{Kind: PacketFromBelow, Packet: &Packet{Type: ACK}}, ErrNoTransition)
	mustReject(t, r, Event{Kind: PacketFromBelow, Packet: &Packet{Type: NAK}}, ErrNoTransition)
	mustReject(t, r, Event{Kind: PacketFromBelow, Packet: dataPacket(2, []byte("no"))}, ErrNoTransition)
	assertReceiverHeld(t, r, WaitFor0FromBelow)

	mustHandle(t, r, Event{Kind: PacketFromBelow, Packet: dataPacket(0, []byte("ok"))})
	mustReject(t, r, Event{Kind: CallFromAbove, Data: []byte("no")}, ErrEventNotAccepted)
	assertReceiverHeld(t, r, WaitFor1FromBelow)

	r.state = 99
	mustReject(t, r, Event{Kind: PacketFromBelow, Packet: dataPacket(0, []byte("x"))}, ErrEventNotAccepted)
}

func TestReceiverDeliverIsolation(t *testing.T) {
	r := NewReceiver()
	payload := []byte("abc")
	pkt := dataPacket(0, payload)
	actions := mustHandle(t, r, Event{Kind: PacketFromBelow, Packet: pkt})
	assertDeliver(t, actions[0], []byte("abc"))

	payload[0] = 'z'
	pkt.Payload[2] = 'z'
	if !bytes.Equal(actions[0].Data, []byte("abc")) {
		t.Fatalf("delivered data = %q", actions[0].Data)
	}
	actions[0].Data[1] = 'y'
	if payload[1] != 'b' || pkt.Payload[1] != 'b' {
		t.Fatal("delivered data aliases the packet payload")
	}
}

func TestReceiverEmptyPayload(t *testing.T) {
	r := NewReceiver()
	actions := mustHandle(t, r, Event{Kind: PacketFromBelow, Packet: &Packet{Type: Data, Seq: 0}})
	assertDelivered(t, actions, nil, ACK)
	if r.State() != WaitFor1FromBelow {
		t.Fatalf("state = %s, want %s", r.State(), WaitFor1FromBelow)
	}
}

func dataPacket(seq uint8, payload []byte) *Packet {
	return &Packet{Type: Data, Seq: seq, Payload: payload}
}

func assertDelivered(t *testing.T, actions []Action, want []byte, reply PacketType) {
	t.Helper()
	if len(actions) != 2 {
		t.Fatalf("actions = %d, want deliver then reply", len(actions))
	}
	assertDeliver(t, actions[0], want)
	pkt := assertSend(t, actions[1], SendPacket)
	if pkt.Type != reply {
		t.Fatalf("reply type = %d, want %d", pkt.Type, reply)
	}
}
