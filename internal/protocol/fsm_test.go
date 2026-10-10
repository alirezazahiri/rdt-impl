package protocol

import (
	"bytes"
	"errors"
	"testing"
)

type machine interface {
	Handle(Event) ([]Action, error)
}

func mustHandle(t *testing.T, m machine, event Event) []Action {
	t.Helper()
	actions, err := m.Handle(event)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	return actions
}

func mustReject(t *testing.T, m machine, event Event, target error) {
	t.Helper()
	actions, err := m.Handle(event)
	if !errors.Is(err, target) {
		t.Fatalf("Handle() error = %v, want %v", err, target)
	}
	if len(actions) != 0 {
		t.Fatalf("Handle() actions = %+v, want none", actions)
	}
}

func assertSend(t *testing.T, action Action, kind ActionKind) *Packet {
	t.Helper()
	if action.Kind != kind {
		t.Fatalf("action = %s, want %s", action.Kind, kind)
	}
	if action.Packet == nil {
		t.Fatal("packet action has no packet")
	}
	if action.Data != nil {
		t.Fatalf("packet action data = %x, want none", action.Data)
	}
	assertChecksum(t, action.Packet)
	return action.Packet
}

func assertDeliver(t *testing.T, action Action, want []byte) {
	t.Helper()
	if action.Kind != DeliverData {
		t.Fatalf("action = %s, want %s", action.Kind, DeliverData)
	}
	if action.Packet != nil {
		t.Fatal("deliver action includes a packet")
	}
	if !bytes.Equal(action.Data, want) {
		t.Fatalf("data = %x, want %x", action.Data, want)
	}
}

func assertChecksum(t *testing.T, pkt *Packet) {
	t.Helper()
	clone := *pkt
	clone.Checksum = 0
	if _, err := clone.Serialize(); err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}
	if clone.Checksum != pkt.Checksum {
		t.Fatalf("checksum = %#x, want %#x", pkt.Checksum, clone.Checksum)
	}
}

func assertSenderHeld(t *testing.T, s *Sender, state SenderState, pkt *Packet) {
	t.Helper()
	if s.State() != state {
		t.Fatalf("state = %s, want %s", s.State(), state)
	}
	if s.Packet() != pkt {
		t.Fatal("stored packet changed")
	}
}

func assertReceiverHeld(t *testing.T, r *Receiver, state ReceiverState) {
	t.Helper()
	if r.State() != state {
		t.Fatalf("state = %s, want %s", r.State(), state)
	}
}
