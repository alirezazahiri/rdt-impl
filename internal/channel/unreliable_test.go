package channel

import (
	"bytes"
	"errors"
	"math"
	"testing"
)

func TestUnreliableSameSeedReproducesCorruption(t *testing.T) {
	packets := [][]byte{
		[]byte("alpha-packet"),
		[]byte("beta-packet-22"),
		nil,
		{},
		bytes.Repeat([]byte{0x5a}, 32),
	}

	first, n1 := deliver(t, 42, 1, packets)
	second, n2 := deliver(t, 42, 1, packets)
	if n1 != n2 || !sameDatagrams(first, second) {
		t.Fatalf("seed 42 produced different patterns: count %d/%d", n1, n2)
	}
	if n1 != 3 {
		t.Fatalf("Corrupted() = %d, want 3 non-empty datagrams", n1)
	}

	other, nOther := deliver(t, 99, 1, packets)
	if n1 == nOther && sameDatagrams(first, other) {
		t.Fatal("different seeds produced the same corruption pattern")
	}
}

func TestUnreliablePartialCorruptionIsStable(t *testing.T) {
	packets := make([][]byte, 40)
	for i := range packets {
		packets[i] = bytes.Repeat([]byte{byte(i + 1)}, 16)
	}

	first, n1 := deliver(t, 7, 0.35, packets)
	second, n2 := deliver(t, 7, 0.35, packets)
	if n1 != n2 || !sameDatagrams(first, second) {
		t.Fatalf("partial corruption was not reproducible: count %d/%d", n1, n2)
	}
	if n1 <= 0 || n1 >= len(packets) {
		t.Fatalf("Corrupted() = %d, want some but not all of %d", n1, len(packets))
	}
}

func TestUnreliableCorruptionUsesACopy(t *testing.T) {
	sent := []byte{0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80}
	original := bytes.Clone(sent)

	ch := mustNew(t, Config{Seed: 3, CorruptProbability: 1})
	if err := ch.Send(sent); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if !bytes.Equal(sent, original) {
		t.Fatalf("Send() modified caller buffer: %x", sent)
	}

	got, err := ch.Receive()
	if err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	if bitDistance(got, original) != 1 {
		t.Fatalf("delivered = %x, want one flipped bit of %x", got, original)
	}

	got[0] ^= 0xff
	if !bytes.Equal(sent, original) {
		t.Fatal("delivered datagram aliases the caller buffer")
	}
	if ch.Corrupted() != 1 {
		t.Fatalf("Corrupted() = %d, want 1", ch.Corrupted())
	}
}

func TestUnreliablePreservesOrderWithoutExtraFaults(t *testing.T) {
	packets := [][]byte{
		[]byte("one"),
		[]byte("two"),
		[]byte("three"),
	}

	ch := mustNew(t, Config{Seed: 1})
	for _, pkt := range packets {
		if err := ch.Send(pkt); err != nil {
			t.Fatalf("Send() error = %v", err)
		}
	}
	if ch.Corrupted() != 0 {
		t.Fatalf("Corrupted() = %d, want 0", ch.Corrupted())
	}

	for _, want := range packets {
		got, err := ch.Receive()
		if err != nil {
			t.Fatalf("Receive() error = %v", err)
		}
		if !bytes.Equal(got, want) || len(got) != len(want) {
			t.Fatalf("delivered = %x, want %x", got, want)
		}
	}
	if _, err := ch.Receive(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("Receive() error = %v, want %v", err, ErrEmpty)
	}
}

func TestUnreliableRejectsDisabledFaults(t *testing.T) {
	cases := []Config{
		{LossProbability: 0.1},
		{DuplicateProbability: 0.2},
		{ReorderProbability: 1},
	}
	for _, cfg := range cases {
		ch, err := NewUnreliable(cfg)
		if !errors.Is(err, ErrFaultDisabled) {
			t.Fatalf("NewUnreliable(%+v) error = %v, want %v", cfg, err, ErrFaultDisabled)
		}
		if ch != nil {
			t.Fatalf("NewUnreliable(%+v) returned a channel with error", cfg)
		}
	}
}

func TestUnreliableRejectsCorruptProbability(t *testing.T) {
	for _, p := range []float64{-0.01, 1.01, math.NaN()} {
		ch, err := NewUnreliable(Config{CorruptProbability: p})
		if !errors.Is(err, ErrCorruptProbability) {
			t.Fatalf("NewUnreliable(p=%v) error = %v, want %v", p, err, ErrCorruptProbability)
		}
		if ch != nil {
			t.Fatalf("NewUnreliable(p=%v) returned a channel with error", p)
		}
	}
}

func deliver(t *testing.T, seed int64, p float64, packets [][]byte) ([][]byte, int) {
	t.Helper()

	ch := mustNew(t, Config{Seed: seed, CorruptProbability: p})
	var link Channel = ch
	for _, pkt := range packets {
		if err := link.Send(pkt); err != nil {
			t.Fatalf("Send() error = %v", err)
		}
	}

	got := make([][]byte, len(packets))
	for i := range packets {
		pkt, err := link.Receive()
		if err != nil {
			t.Fatalf("Receive() error = %v", err)
		}
		got[i] = pkt
	}
	if _, err := link.Receive(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("Receive() error = %v, want %v", err, ErrEmpty)
	}
	return got, ch.Corrupted()
}

func mustNew(t *testing.T, cfg Config) *Unreliable {
	t.Helper()
	ch, err := NewUnreliable(cfg)
	if err != nil {
		t.Fatalf("NewUnreliable() error = %v", err)
	}
	return ch
}

func sameDatagrams(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if (a[i] == nil) != (b[i] == nil) || !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func bitDistance(a, b []byte) int {
	if len(a) != len(b) {
		return -1
	}
	n := 0
	for i := range a {
		x := a[i] ^ b[i]
		for x != 0 {
			n += int(x & 1)
			x >>= 1
		}
	}
	return n
}
