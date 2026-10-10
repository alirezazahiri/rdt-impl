package protocol

import (
	"bytes"
	"testing"
)

func TestInMemoryExchange(t *testing.T) {
	sender := NewSender()
	receiver := NewReceiver()

	// Sequence 0: damaged data, then a damaged ACK, then the duplicate.
	pkt0 := sendData(t, sender, []byte("one"), 0)
	reply, data := receiveWire(t, receiver, pkt0, true)
	if reply.Type != NAK || data != nil {
		t.Fatalf("corrupt data reply = type %d data %q, want NAK and no delivery", reply.Type, data)
	}
	retransmit(t, sender, &Packet{Type: NAK}, pkt0)
	assertSenderHeld(t, sender, WaitForACKOrNAK0, pkt0)

	reply, data = receiveWire(t, receiver, pkt0, false)
	if reply.Type != ACK || !bytes.Equal(data, []byte("one")) {
		t.Fatalf("delivery = type %d data %q", reply.Type, data)
	}
	if receiver.State() != WaitFor1FromBelow {
		t.Fatalf("receiver state = %s", receiver.State())
	}

	corruptFeedback(t, sender, pkt0)
	reply, data = receiveWire(t, receiver, sender.Packet(), false)
	if reply.Type != ACK || data != nil {
		t.Fatalf("duplicate reply = type %d data %q, want ACK and no delivery", reply.Type, data)
	}
	if receiver.State() != WaitFor1FromBelow {
		t.Fatalf("duplicate changed receiver state to %s", receiver.State())
	}
	acknowledge(t, sender, reply, WaitForCall1)

	// Sequence 1: damaged data, NAK, then a clean delivery back to the start.
	pkt1 := sendData(t, sender, []byte("two"), 1)
	reply, data = receiveWire(t, receiver, pkt1, true)
	if reply.Type != NAK || data != nil {
		t.Fatalf("corrupt data reply = type %d data %q, want NAK and no delivery", reply.Type, data)
	}
	retransmit(t, sender, &Packet{Type: NAK, Seq: 1}, pkt1)

	reply, data = receiveWire(t, receiver, pkt1, false)
	if reply.Type != ACK || !bytes.Equal(data, []byte("two")) {
		t.Fatalf("delivery = type %d data %q", reply.Type, data)
	}
	acknowledge(t, sender, reply, WaitForCall0)
	if receiver.State() != WaitFor0FromBelow {
		t.Fatalf("receiver state = %s, want %s", receiver.State(), WaitFor0FromBelow)
	}
	if sender.Packet() != nil {
		t.Fatal("sender still holds a packet")
	}
}

func sendData(t *testing.T, s *Sender, data []byte, seq uint8) *Packet {
	t.Helper()
	pkt := onePacket(t, mustHandle(t, s, Event{Kind: CallFromAbove, Data: data}), SendPacket)
	if pkt.Seq != seq || !bytes.Equal(pkt.Payload, data) {
		t.Fatalf("sent {Seq:%d Payload:%q}, want {Seq:%d Payload:%q}", pkt.Seq, pkt.Payload, seq, data)
	}
	return pkt
}

func retransmit(t *testing.T, s *Sender, feedback *Packet, want *Packet) {
	t.Helper()
	feedbackOf(t, s, Event{Kind: PacketFromBelow, Packet: feedback}, want)
}

func corruptFeedback(t *testing.T, s *Sender, want *Packet) {
	t.Helper()
	feedbackOf(t, s, Event{Kind: PacketFromBelow, Corrupt: true}, want)
}

func feedbackOf(t *testing.T, s *Sender, event Event, want *Packet) {
	t.Helper()
	got := onePacket(t, mustHandle(t, s, event), Retransmit)
	if got != want {
		t.Fatal("retransmit replaced the stored packet")
	}
}

func acknowledge(t *testing.T, s *Sender, ack *Packet, next SenderState) {
	t.Helper()
	actions := mustHandle(t, s, Event{Kind: PacketFromBelow, Packet: ack})
	if len(actions) != 0 {
		t.Fatalf("ACK actions = %+v, want none", actions)
	}
	assertSenderHeld(t, s, next, nil)
}

func receiveWire(t *testing.T, r *Receiver, pkt *Packet, corrupt bool) (*Packet, []byte) {
	t.Helper()
	before := r.State()
	actions := mustHandle(t, r, eventFromWire(t, pkt, corrupt))
	if corrupt {
		if r.State() != before {
			t.Fatalf("corrupt packet changed state from %s to %s", before, r.State())
		}
		return onePacket(t, actions, SendPacket), nil
	}
	if len(actions) == 1 {
		return onePacket(t, actions, SendPacket), nil
	}
	if len(actions) != 2 {
		t.Fatalf("actions = %d, want 1 or 2", len(actions))
	}
	assertDeliver(t, actions[0], pkt.Payload)
	return assertSend(t, actions[1], SendPacket), actions[0].Data
}

// eventFromWire stands in for the socket reader. It serializes the packet
// and, when corrupt is set, flips a byte so the checksum fails.
func eventFromWire(t *testing.T, pkt *Packet, corrupt bool) Event {
	t.Helper()
	raw, err := pkt.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}
	if corrupt {
		raw[len(raw)-1] ^= 0xFF
		if _, err := DecodePacket(raw); err == nil {
			t.Fatal("damaged packet decoded")
		}
		return Event{Kind: PacketFromBelow, Corrupt: true}
	}
	decoded, err := DecodePacket(raw)
	if err != nil {
		t.Fatalf("DecodePacket() error = %v", err)
	}
	return Event{Kind: PacketFromBelow, Packet: decoded}
}
