package channel

import (
	"bytes"
	"errors"
	"math"
	"math/rand"
	"sync"
)

// Config selects the faults applied by an unreliable channel.
// Loss, duplication, and reordering are disabled and must stay zero.
type Config struct {
	// Seed initializes the random source. The same seed and the same
	// sequence of Send calls produce the same corruption.
	Seed int64

	// CorruptProbability is the chance that Send flips one bit in a copy
	// of a non-empty datagram. Zero leaves every datagram intact. One
	// corrupts every non-empty datagram. Values outside [0, 1] are rejected.
	CorruptProbability float64

	// LossProbability must be zero. Datagrams are not dropped.
	LossProbability float64

	// DuplicateProbability must be zero. Datagrams are not duplicated.
	DuplicateProbability float64

	// ReorderProbability must be zero. Datagrams stay in send order.
	ReorderProbability float64
}

var (
	// ErrFaultDisabled is returned when Config enables loss, duplication,
	// or reordering.
	ErrFaultDisabled = errors.New("loss, duplication, and reordering are disabled")

	// ErrCorruptProbability is returned when CorruptProbability is outside [0, 1].
	ErrCorruptProbability = errors.New("corrupt probability out of range")
)

// Unreliable is a one-way channel. Send queues a copy of a datagram,
// and Receive returns queued datagrams in send order. A non-empty copy
// may have one bit flipped. The caller's buffer is never modified.
//
// A two-way link is two Unreliable values, one in each direction.
type Unreliable struct {
	mu        sync.Mutex
	rng       *rand.Rand
	corruptP  float64
	corrupted int
	queue     [][]byte
}

// NewUnreliable returns a channel whose corruption follows cfg.Seed.
func NewUnreliable(cfg Config) (*Unreliable, error) {
	if math.IsNaN(cfg.CorruptProbability) || cfg.CorruptProbability < 0 || cfg.CorruptProbability > 1 {
		return nil, ErrCorruptProbability
	}
	if cfg.LossProbability != 0 || cfg.DuplicateProbability != 0 || cfg.ReorderProbability != 0 {
		return nil, ErrFaultDisabled
	}

	return &Unreliable{
		rng:      rand.New(rand.NewSource(cfg.Seed)),
		corruptP: cfg.CorruptProbability,
	}, nil
}

// Send queues a copy of data. When corruption is selected, one bit in
// that copy is flipped. data is left unchanged. Every datagram is queued
// once, in order.
func (u *Unreliable) Send(data []byte) error {
	packet := bytes.Clone(data)

	u.mu.Lock()
	defer u.mu.Unlock()

	if u.corrupt(packet) {
		u.corrupted++
	}
	u.queue = append(u.queue, packet)
	return nil
}

// Receive returns the next datagram queued by Send.
// The caller owns the returned slice.
func (u *Unreliable) Receive() ([]byte, error) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if len(u.queue) == 0 {
		return nil, ErrEmpty
	}

	packet := u.queue[0]
	u.queue[0] = nil
	u.queue = u.queue[1:]
	return packet, nil
}

// Corrupted reports how many datagrams Send corrupted.
func (u *Unreliable) Corrupted() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.corrupted
}

// corrupt flips one bit of packet when the seeded source selects it.
// The random draw happens only for a non-empty packet while corruption
// is enabled, so a seed fixes the pattern for a given sequence of sends.
func (u *Unreliable) corrupt(packet []byte) bool {
	if u.corruptP == 0 || len(packet) == 0 {
		return false
	}
	if u.rng.Float64() >= u.corruptP {
		return false
	}

	index := u.rng.Intn(len(packet))
	bit := u.rng.Intn(8)
	packet[index] ^= 1 << bit
	return true
}

var _ Channel = (*Unreliable)(nil)
