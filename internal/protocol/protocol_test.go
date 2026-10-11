package protocol

import (
	"bytes"
	"testing"
)

func TestSuccessfulPacket0AndPacket1Delivery(t *testing.T) {
	sender := NewSender()
	receiver := NewReceiver()

	exchangeDeliver(t, sender, receiver, []byte("packet-0"), 0, WaitForCall1, WaitFor1FromBelow)
	exchangeDeliver(t, sender, receiver, []byte("packet-1"), 1, WaitForCall0, WaitFor0FromBelow)

	if sender.Packet() != nil {
		t.Fatal("sender still holds a packet")
	}
}

func TestCorruptDataPacketNAKAndRetransmission(t *testing.T) {
	sender := NewSender()
	receiver := NewReceiver()
	payloads := [][]byte{[]byte("packet-0"), []byte("packet-1")}
	var got [][]byte

	for seq := uint8(0); seq < 2; seq++ {
		payload := payloads[seq]
		pkt := sendData(t, sender, payload, seq)
		for attempt := 1; attempt <= 2; attempt++ {
			reply, data := receiveWire(t, receiver, pkt, true)
			if reply.Type != NAK || data != nil {
				t.Fatalf("sequence %d attempt %d reply = type %d data %q, want NAK and no delivery", seq, attempt, reply.Type, data)
			}
			assertReceiverHeld(t, receiver, receiverExpecting(seq))
			retransmit(t, sender, reply, pkt)
			assertSenderHeld(t, sender, waitingForACK(seq), pkt)
		}

		reply, data := receiveWire(t, receiver, pkt, false)
		if reply.Type != ACK || !bytes.Equal(data, payload) {
			t.Fatalf("sequence %d delivery = type %d data %q", seq, reply.Type, data)
		}
		got = append(got, bytes.Clone(data))
		if receiver.State() != receiverAfterDelivery(seq) {
			t.Fatalf("receiver state = %s, want %s", receiver.State(), receiverAfterDelivery(seq))
		}
		acknowledge(t, sender, reply, senderAfterACK(seq))
	}

	if len(got) != 2 || !bytes.Equal(got[0], payloads[0]) || !bytes.Equal(got[1], payloads[1]) {
		t.Fatalf("delivered = %q, want packet-0 then packet-1", got)
	}
}

func TestCorruptACKRetransmission(t *testing.T) {
	sender := NewSender()
	receiver := NewReceiver()
	payloads := [][]byte{[]byte("packet-0"), []byte("packet-1")}

	for seq := uint8(0); seq < 2; seq++ {
		payload := payloads[seq]
		pkt := sendData(t, sender, payload, seq)
		reply, data := receiveWire(t, receiver, pkt, false)
		if reply.Type != ACK || !bytes.Equal(data, payload) {
			t.Fatalf("sequence %d delivery = type %d data %q", seq, reply.Type, data)
		}
		delivered := 1

		for attempt := 1; attempt <= 2; attempt++ {
			got := onePacket(t, mustHandle(t, sender, eventFromWire(t, reply, true)), Retransmit)
			if got != pkt {
				t.Fatalf("sequence %d attempt %d retransmit replaced the stored packet", seq, attempt)
			}
			assertSenderHeld(t, sender, waitingForACK(seq), pkt)

			next, again := receiveWire(t, receiver, pkt, false)
			if again != nil {
				delivered++
			}
			if next.Type != ACK {
				t.Fatalf("sequence %d duplicate reply = type %d, want ACK", seq, next.Type)
			}
			assertReceiverHeld(t, receiver, receiverAfterDelivery(seq))
			reply = next
		}
		if delivered != 1 {
			t.Fatalf("sequence %d delivered %d times", seq, delivered)
		}
		acknowledge(t, sender, reply, senderAfterACK(seq))
	}

	if sender.State() != WaitForCall0 || sender.Packet() != nil {
		t.Fatalf("sender state = %s", sender.State())
	}
	assertReceiverHeld(t, receiver, WaitFor0FromBelow)
}

func TestDuplicatePacketsAreNotDeliveredTwice(t *testing.T) {
	sender := NewSender()
	receiver := NewReceiver()

	stale := mustPacket(t, Data, 1, []byte("stale"))
	if data := replayData(t, receiver, stale); data != nil {
		t.Fatalf("stale sequence 1 delivered %q", data)
	}

	var got [][]byte
	pkt0 := sendData(t, sender, []byte("packet-0"), 0)
	reply, data := receiveWire(t, receiver, pkt0, false)
	if data == nil {
		t.Fatal("sequence 0 was not delivered")
	}
	got = append(got, bytes.Clone(data))
	if again := replayData(t, receiver, pkt0); again != nil {
		t.Fatalf("sequence 0 delivered twice: %q", again)
	}
	if again := replayData(t, receiver, mustPacket(t, Data, 0, []byte("replacement"))); again != nil {
		t.Fatalf("altered sequence 0 duplicate delivered %q", again)
	}
	acknowledge(t, sender, reply, WaitForCall1)
	if again := replayData(t, receiver, pkt0); again != nil {
		t.Fatalf("sequence 0 delivered after acknowledgement: %q", again)
	}

	pkt1 := sendData(t, sender, []byte("packet-1"), 1)
	reply, data = receiveWire(t, receiver, pkt1, false)
	if data == nil {
		t.Fatal("sequence 1 was not delivered")
	}
	got = append(got, bytes.Clone(data))
	if again := replayData(t, receiver, pkt1); again != nil {
		t.Fatalf("sequence 1 delivered twice: %q", again)
	}
	if again := replayData(t, receiver, mustPacket(t, Data, 1, []byte("replacement"))); again != nil {
		t.Fatalf("altered sequence 1 duplicate delivered %q", again)
	}
	acknowledge(t, sender, reply, WaitForCall0)
	if again := replayData(t, receiver, pkt1); again != nil {
		t.Fatalf("sequence 1 delivered after acknowledgement: %q", again)
	}

	if len(got) != 2 || !bytes.Equal(got[0], []byte("packet-0")) || !bytes.Equal(got[1], []byte("packet-1")) {
		t.Fatalf("delivered = %q, want packet-0 then packet-1", got)
	}
	assertSenderHeld(t, sender, WaitForCall0, nil)
	assertReceiverHeld(t, receiver, WaitFor0FromBelow)
}

func TestUnexpectedACKDoesNotAdvanceSender(t *testing.T) {
	sender := NewSender()
	receiver := NewReceiver()

	// Nothing is outstanding, so an ACK cannot apply.
	idleACK := mustPacket(t, ACK, 0, nil)
	mustReject(t, sender, Event{Kind: PacketFromBelow, Packet: idleACK}, ErrEventNotAccepted)
	mustReject(t, sender, Event{Kind: PacketFromBelow, Corrupt: true, Packet: idleACK}, ErrEventNotAccepted)
	assertSenderHeld(t, sender, WaitForCall0, nil)

	payloads := [][]byte{[]byte("packet-0"), []byte("packet-1")}
	for seq := uint8(0); seq < 2; seq++ {
		pkt := sendData(t, sender, payloads[seq], seq)
		first, data := receiveWire(t, receiver, pkt, false)
		if !bytes.Equal(data, payloads[seq]) {
			t.Fatalf("sequence %d data = %q", seq, data)
		}

		got := onePacket(t, mustHandle(t, sender, eventFromWire(t, first, true)), Retransmit)
		if got != pkt {
			t.Fatalf("sequence %d corrupt ACK replaced the stored packet", seq)
		}
		assertSenderHeld(t, sender, waitingForACK(seq), pkt)

		second, again := receiveWire(t, receiver, pkt, false)
		if again != nil {
			t.Fatalf("sequence %d duplicate delivered %q", seq, again)
		}
		acknowledge(t, sender, second, senderAfterACK(seq))

		// first was already superseded. A late copy must leave the sender idle.
		mustReject(t, sender, Event{Kind: PacketFromBelow, Packet: first}, ErrEventNotAccepted)
		assertSenderHeld(t, sender, senderAfterACK(seq), nil)
	}

	exchangeDeliver(t, sender, receiver, []byte("again"), 0, WaitForCall1, WaitFor1FromBelow)
}

func TestProtocolSenderTransitions(t *testing.T) {
	ack := &Packet{Type: ACK}
	nak := &Packet{Type: NAK}

	tests := []senderTransition{
		{
			name:    "call 0 sends sequence 0",
			start:   WaitForCall0,
			event:   Event{Kind: CallFromAbove, Data: []byte("s0")},
			action:  SendPacket,
			next:    WaitForACKOrNAK0,
			seq:     0,
			payload: []byte("s0"),
		},
		{
			name:   "corrupt arrival retransmits sequence 0",
			start:  WaitForACKOrNAK0,
			event:  Event{Kind: PacketFromBelow, Corrupt: true},
			action: Retransmit,
			next:   WaitForACKOrNAK0,
		},
		{
			name:   "corrupt ACK retransmits sequence 0",
			start:  WaitForACKOrNAK0,
			event:  Event{Kind: PacketFromBelow, Corrupt: true, Packet: ack},
			action: Retransmit,
			next:   WaitForACKOrNAK0,
		},
		{
			name:   "NAK retransmits sequence 0",
			start:  WaitForACKOrNAK0,
			event:  Event{Kind: PacketFromBelow, Packet: nak},
			action: Retransmit,
			next:   WaitForACKOrNAK0,
		},
		{
			name:  "ACK completes sequence 0",
			start: WaitForACKOrNAK0,
			event: Event{Kind: PacketFromBelow, Packet: ack},
			next:  WaitForCall1,
		},
		{
			name:    "call 1 sends sequence 1",
			start:   WaitForCall1,
			event:   Event{Kind: CallFromAbove, Data: []byte("s1")},
			action:  SendPacket,
			next:    WaitForACKOrNAK1,
			seq:     1,
			payload: []byte("s1"),
		},
		{
			name:   "corrupt arrival retransmits sequence 1",
			start:  WaitForACKOrNAK1,
			event:  Event{Kind: PacketFromBelow, Corrupt: true},
			action: Retransmit,
			next:   WaitForACKOrNAK1,
		},
		{
			name:   "corrupt ACK retransmits sequence 1",
			start:  WaitForACKOrNAK1,
			event:  Event{Kind: PacketFromBelow, Corrupt: true, Packet: ack},
			action: Retransmit,
			next:   WaitForACKOrNAK1,
		},
		{
			name:   "NAK retransmits sequence 1",
			start:  WaitForACKOrNAK1,
			event:  Event{Kind: PacketFromBelow, Packet: nak},
			action: Retransmit,
			next:   WaitForACKOrNAK1,
		},
		{
			name:  "ACK completes sequence 1",
			start: WaitForACKOrNAK1,
			event: Event{Kind: PacketFromBelow, Packet: ack},
			next:  WaitForCall0,
		},
		{
			name:    "ACK while waiting for call 0",
			start:   WaitForCall0,
			event:   Event{Kind: PacketFromBelow, Packet: ack},
			wantErr: ErrEventNotAccepted,
		},
		{
			name:    "NAK while waiting for call 0",
			start:   WaitForCall0,
			event:   Event{Kind: PacketFromBelow, Packet: nak},
			wantErr: ErrEventNotAccepted,
		},
		{
			name:    "data while waiting for call 0",
			start:   WaitForCall0,
			event:   Event{Kind: PacketFromBelow, Packet: dataPacket(0, []byte("s0"))},
			wantErr: ErrEventNotAccepted,
		},
		{
			name:    "corrupt ACK while waiting for call 0",
			start:   WaitForCall0,
			event:   Event{Kind: PacketFromBelow, Corrupt: true, Packet: ack},
			wantErr: ErrEventNotAccepted,
		},
		{
			name:    "unknown event while waiting for call 0",
			start:   WaitForCall0,
			event:   Event{},
			wantErr: ErrUnknownEvent,
		},
		{
			name:    "ACK while waiting for call 1",
			start:   WaitForCall1,
			event:   Event{Kind: PacketFromBelow, Packet: ack},
			wantErr: ErrEventNotAccepted,
		},
		{
			name:    "NAK while waiting for call 1",
			start:   WaitForCall1,
			event:   Event{Kind: PacketFromBelow, Packet: nak},
			wantErr: ErrEventNotAccepted,
		},
		{
			name:    "data while waiting for call 1",
			start:   WaitForCall1,
			event:   Event{Kind: PacketFromBelow, Packet: dataPacket(1, []byte("s1"))},
			wantErr: ErrEventNotAccepted,
		},
		{
			name:    "corrupt ACK while waiting for call 1",
			start:   WaitForCall1,
			event:   Event{Kind: PacketFromBelow, Corrupt: true, Packet: ack},
			wantErr: ErrEventNotAccepted,
		},
		{
			name:    "unknown event while waiting for call 1",
			start:   WaitForCall1,
			event:   Event{},
			wantErr: ErrUnknownEvent,
		},
		{
			name:    "call while waiting for ACK 0",
			start:   WaitForACKOrNAK0,
			event:   Event{Kind: CallFromAbove, Data: []byte("blocked")},
			wantErr: ErrEventNotAccepted,
		},
		{
			name:    "missing packet while waiting for ACK 0",
			start:   WaitForACKOrNAK0,
			event:   Event{Kind: PacketFromBelow},
			wantErr: ErrMissingPacket,
		},
		{
			name:    "data while waiting for ACK 0",
			start:   WaitForACKOrNAK0,
			event:   Event{Kind: PacketFromBelow, Packet: dataPacket(0, []byte("s0"))},
			wantErr: ErrNoTransition,
		},
		{
			name:    "unknown event while waiting for ACK 0",
			start:   WaitForACKOrNAK0,
			event:   Event{},
			wantErr: ErrUnknownEvent,
		},
		{
			name:    "call while waiting for ACK 1",
			start:   WaitForACKOrNAK1,
			event:   Event{Kind: CallFromAbove, Data: []byte("blocked")},
			wantErr: ErrEventNotAccepted,
		},
		{
			name:    "missing packet while waiting for ACK 1",
			start:   WaitForACKOrNAK1,
			event:   Event{Kind: PacketFromBelow},
			wantErr: ErrMissingPacket,
		},
		{
			name:    "data while waiting for ACK 1",
			start:   WaitForACKOrNAK1,
			event:   Event{Kind: PacketFromBelow, Packet: dataPacket(1, []byte("s1"))},
			wantErr: ErrNoTransition,
		},
		{
			name:    "unknown event while waiting for ACK 1",
			start:   WaitForACKOrNAK1,
			event:   Event{},
			wantErr: ErrUnknownEvent,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runSenderTransition(t, tc)
		})
	}

	t.Run("undefined state rejects events", func(t *testing.T) {
		s := NewSender()
		s.state = 99
		mustReject(t, s, Event{Kind: CallFromAbove, Data: []byte{1}}, ErrEventNotAccepted)
		if s.state != 99 || s.sndpkt != nil {
			t.Fatalf("state = %s packet held = %v", s.State(), s.sndpkt != nil)
		}
	})
}

func TestProtocolReceiverTransitions(t *testing.T) {
	tests := []receiverTransition{
		{
			name:  "corrupt packet while waiting for 0 sends NAK",
			start: WaitFor0FromBelow,
			event: Event{Kind: PacketFromBelow, Corrupt: true, Packet: dataPacket(0, []byte("hidden"))},
			reply: NAK,
			next:  WaitFor0FromBelow,
		},
		{
			name:  "sequence 1 while waiting for 0 is a duplicate",
			start: WaitFor0FromBelow,
			event: Event{Kind: PacketFromBelow, Packet: dataPacket(1, []byte("duplicate"))},
			reply: ACK,
			next:  WaitFor0FromBelow,
		},
		{
			name:    "sequence 0 is delivered",
			start:   WaitFor0FromBelow,
			event:   Event{Kind: PacketFromBelow, Packet: dataPacket(0, []byte("seq-0"))},
			deliver: true,
			payload: []byte("seq-0"),
			reply:   ACK,
			next:    WaitFor1FromBelow,
		},
		{
			name:  "corrupt packet while waiting for 1 sends NAK",
			start: WaitFor1FromBelow,
			event: Event{Kind: PacketFromBelow, Corrupt: true, Packet: dataPacket(1, []byte("hidden"))},
			reply: NAK,
			next:  WaitFor1FromBelow,
		},
		{
			name:  "sequence 0 while waiting for 1 is a duplicate",
			start: WaitFor1FromBelow,
			event: Event{Kind: PacketFromBelow, Packet: dataPacket(0, []byte("duplicate"))},
			reply: ACK,
			next:  WaitFor1FromBelow,
		},
		{
			name:    "sequence 1 is delivered",
			start:   WaitFor1FromBelow,
			event:   Event{Kind: PacketFromBelow, Packet: dataPacket(1, []byte("seq-1"))},
			deliver: true,
			payload: []byte("seq-1"),
			reply:   ACK,
			next:    WaitFor0FromBelow,
		},
		{
			name:    "call while waiting for 0",
			start:   WaitFor0FromBelow,
			event:   Event{Kind: CallFromAbove, Data: []byte("no")},
			wantErr: ErrEventNotAccepted,
		},
		{
			name:    "unknown event while waiting for 0",
			start:   WaitFor0FromBelow,
			event:   Event{},
			wantErr: ErrUnknownEvent,
		},
		{
			name:    "missing packet while waiting for 0",
			start:   WaitFor0FromBelow,
			event:   Event{Kind: PacketFromBelow},
			wantErr: ErrMissingPacket,
		},
		{
			name:    "ACK while waiting for 0",
			start:   WaitFor0FromBelow,
			event:   Event{Kind: PacketFromBelow, Packet: &Packet{Type: ACK}},
			wantErr: ErrNoTransition,
		},
		{
			name:    "NAK while waiting for 0",
			start:   WaitFor0FromBelow,
			event:   Event{Kind: PacketFromBelow, Packet: &Packet{Type: NAK}},
			wantErr: ErrNoTransition,
		},
		{
			name:    "unexpected sequence while waiting for 0",
			start:   WaitFor0FromBelow,
			event:   Event{Kind: PacketFromBelow, Packet: dataPacket(2, []byte("no"))},
			wantErr: ErrNoTransition,
		},
		{
			name:    "call while waiting for 1",
			start:   WaitFor1FromBelow,
			event:   Event{Kind: CallFromAbove, Data: []byte("no")},
			wantErr: ErrEventNotAccepted,
		},
		{
			name:    "unknown event while waiting for 1",
			start:   WaitFor1FromBelow,
			event:   Event{},
			wantErr: ErrUnknownEvent,
		},
		{
			name:    "missing packet while waiting for 1",
			start:   WaitFor1FromBelow,
			event:   Event{Kind: PacketFromBelow},
			wantErr: ErrMissingPacket,
		},
		{
			name:    "ACK while waiting for 1",
			start:   WaitFor1FromBelow,
			event:   Event{Kind: PacketFromBelow, Packet: &Packet{Type: ACK}},
			wantErr: ErrNoTransition,
		},
		{
			name:    "NAK while waiting for 1",
			start:   WaitFor1FromBelow,
			event:   Event{Kind: PacketFromBelow, Packet: &Packet{Type: NAK}},
			wantErr: ErrNoTransition,
		},
		{
			name:    "unexpected sequence while waiting for 1",
			start:   WaitFor1FromBelow,
			event:   Event{Kind: PacketFromBelow, Packet: dataPacket(255, []byte("no"))},
			wantErr: ErrNoTransition,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runReceiverTransition(t, tc)
		})
	}

	t.Run("undefined state rejects events", func(t *testing.T) {
		r := NewReceiver()
		r.state = 99
		mustReject(t, r, Event{Kind: PacketFromBelow, Packet: dataPacket(0, []byte("x"))}, ErrEventNotAccepted)
		if r.state != 99 {
			t.Fatalf("state = %s", r.State())
		}
	})
}

type senderTransition struct {
	name    string
	start   SenderState
	event   Event
	wantErr error
	action  ActionKind
	next    SenderState
	seq     uint8
	payload []byte
}

type receiverTransition struct {
	name    string
	start   ReceiverState
	event   Event
	wantErr error
	deliver bool
	payload []byte
	reply   PacketType
	next    ReceiverState
}

func runSenderTransition(t *testing.T, tc senderTransition) {
	t.Helper()
	s, stored := senderIn(t, tc.start)
	if tc.wantErr != nil {
		mustReject(t, s, tc.event, tc.wantErr)
		assertSenderHeld(t, s, tc.start, stored)
		return
	}

	actions := mustHandle(t, s, tc.event)
	if tc.action == 0 {
		if len(actions) != 0 {
			t.Fatalf("actions = %+v, want none", actions)
		}
		assertSenderHeld(t, s, tc.next, nil)
		return
	}

	pkt := onePacket(t, actions, tc.action)
	if tc.action == Retransmit {
		if pkt != stored {
			t.Fatal("retransmit replaced the stored packet")
		}
		assertSenderHeld(t, s, tc.next, stored)
		return
	}
	if pkt.Type != Data || pkt.Seq != tc.seq || !bytes.Equal(pkt.Payload, tc.payload) {
		t.Fatalf("packet = {Type:%d Seq:%d Payload:%q}", pkt.Type, pkt.Seq, pkt.Payload)
	}
	if s.State() != tc.next {
		t.Fatalf("state = %s, want %s", s.State(), tc.next)
	}
	if s.Packet() != pkt {
		t.Fatal("send did not keep the packet")
	}
}

func runReceiverTransition(t *testing.T, tc receiverTransition) {
	t.Helper()
	r := receiverIn(t, tc.start)
	if tc.wantErr != nil {
		mustReject(t, r, tc.event, tc.wantErr)
		assertReceiverHeld(t, r, tc.start)
		return
	}

	actions := mustHandle(t, r, tc.event)
	if tc.deliver {
		assertDelivered(t, actions, tc.payload, tc.reply)
	} else {
		pkt := onePacket(t, actions, SendPacket)
		if pkt.Type != tc.reply || pkt.Seq != 0 || len(pkt.Payload) != 0 {
			t.Fatalf("reply = {Type:%d Seq:%d Payload:%x}", pkt.Type, pkt.Seq, pkt.Payload)
		}
	}
	if tc.deliver && (actions[1].Packet.Seq != 0 || len(actions[1].Packet.Payload) != 0) {
		t.Fatalf("reply = {Seq:%d Payload:%x}", actions[1].Packet.Seq, actions[1].Packet.Payload)
	}
	if r.State() != tc.next {
		t.Fatalf("state = %s, want %s", r.State(), tc.next)
	}
}

func exchangeDeliver(t *testing.T, s *Sender, r *Receiver, payload []byte, seq uint8, senderNext SenderState, receiverNext ReceiverState) {
	t.Helper()
	pkt := sendData(t, s, payload, seq)
	assertSenderHeld(t, s, waitingForACK(seq), pkt)

	reply, data := receiveWire(t, r, pkt, false)
	if reply.Type != ACK || !bytes.Equal(data, payload) {
		t.Fatalf("sequence %d delivery = type %d data %q", seq, reply.Type, data)
	}
	if r.State() != receiverNext {
		t.Fatalf("receiver state = %s, want %s", r.State(), receiverNext)
	}
	acknowledge(t, s, reply, senderNext)
}

func replayData(t *testing.T, r *Receiver, pkt *Packet) []byte {
	t.Helper()
	before := r.State()
	reply, data := receiveWire(t, r, pkt, false)
	if reply.Type != ACK {
		t.Fatalf("duplicate reply = type %d, want ACK", reply.Type)
	}
	if r.State() != before {
		t.Fatalf("duplicate changed receiver from %s to %s", before, r.State())
	}
	return data
}

func mustPacket(t *testing.T, typ PacketType, seq uint8, payload []byte) *Packet {
	t.Helper()
	pkt, err := makePacket(typ, seq, payload)
	if err != nil {
		t.Fatalf("makePacket() error = %v", err)
	}
	return pkt
}

func senderIn(t *testing.T, state SenderState) (*Sender, *Packet) {
	t.Helper()
	s := NewSender()
	var pkt *Packet
	switch state {
	case WaitForCall0:
	case WaitForACKOrNAK0:
		pkt = onePacket(t, mustHandle(t, s, Event{Kind: CallFromAbove, Data: []byte("p0")}), SendPacket)
	case WaitForCall1:
		onePacket(t, mustHandle(t, s, Event{Kind: CallFromAbove, Data: []byte("p0")}), SendPacket)
		acknowledge(t, s, &Packet{Type: ACK}, WaitForCall1)
	case WaitForACKOrNAK1:
		onePacket(t, mustHandle(t, s, Event{Kind: CallFromAbove, Data: []byte("p0")}), SendPacket)
		acknowledge(t, s, &Packet{Type: ACK}, WaitForCall1)
		pkt = onePacket(t, mustHandle(t, s, Event{Kind: CallFromAbove, Data: []byte("p1")}), SendPacket)
	default:
		t.Fatalf("unsupported sender state %s", state)
		return nil, nil
	}
	if s.State() != state {
		t.Fatalf("setup state = %s, want %s", s.State(), state)
	}
	if s.Packet() != pkt {
		t.Fatal("setup stored the wrong packet")
	}
	return s, pkt
}

func receiverIn(t *testing.T, state ReceiverState) *Receiver {
	t.Helper()
	r := NewReceiver()
	switch state {
	case WaitFor0FromBelow:
	case WaitFor1FromBelow:
		mustHandle(t, r, Event{Kind: PacketFromBelow, Packet: dataPacket(0, []byte("p0"))})
	default:
		t.Fatalf("unsupported receiver state %s", state)
		return nil
	}
	if r.State() != state {
		t.Fatalf("setup state = %s, want %s", r.State(), state)
	}
	return r
}

func waitingForACK(seq uint8) SenderState {
	if seq == 0 {
		return WaitForACKOrNAK0
	}
	return WaitForACKOrNAK1
}

func senderAfterACK(seq uint8) SenderState {
	if seq == 0 {
		return WaitForCall1
	}
	return WaitForCall0
}

func receiverExpecting(seq uint8) ReceiverState {
	if seq == 0 {
		return WaitFor0FromBelow
	}
	return WaitFor1FromBelow
}

func receiverAfterDelivery(seq uint8) ReceiverState {
	if seq == 0 {
		return WaitFor1FromBelow
	}
	return WaitFor0FromBelow
}
