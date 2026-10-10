package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestPacketRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		pkt  Packet
	}{
		{name: "data", pkt: Packet{Type: Data, Seq: 1, Payload: []byte("hello")}},
		{name: "nil payload", pkt: Packet{Type: Data, Seq: 0, Payload: nil}},
		{name: "empty payload", pkt: Packet{Type: ACK, Seq: 2, Payload: []byte{}}},
		{name: "ack", pkt: Packet{Type: ACK, Seq: 7}},
		{name: "nak", pkt: Packet{Type: NAK, Seq: 8, Payload: []byte{0x00, 0xff}}},
		{name: "seq zero", pkt: Packet{Type: Data, Seq: 0, Payload: []byte{0x01}}},
		{name: "seq max", pkt: Packet{Type: NAK, Seq: 255, Payload: []byte{1, 2, 3, 4}}},
		{
			name: "binary payload",
			pkt:  Packet{Type: Data, Seq: 9, Payload: bytes.Repeat([]byte{0x00, 0x01, 0xff}, 50)},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			original := tc.pkt
			raw, err := original.Serialize()
			if err != nil {
				t.Fatalf("Serialize() error = %v", err)
			}

			wireChecksum := binary.BigEndian.Uint32(raw[len(raw)-4:])
			if original.Checksum != wireChecksum {
				t.Fatalf("Serialize() Checksum = %#x, wire checksum = %#x", original.Checksum, wireChecksum)
			}

			var got Packet
			if err := got.Deserialize(raw); err != nil {
				t.Fatalf("Deserialize() error = %v", err)
			}
			assertPacketEqual(t, got, original)
		})
	}
}

func TestPacketRoundTripPayloadLengths(t *testing.T) {
	for n := 0; n <= 64; n++ {
		payload := make([]byte, n)
		for i := range payload {
			payload[i] = byte(i * 3)
		}

		original := Packet{Type: Data, Seq: byte(n), Payload: payload}
		raw, err := original.Serialize()
		if err != nil {
			t.Fatalf("len %d: Serialize() error = %v", n, err)
		}

		var got Packet
		if err := got.Deserialize(raw); err != nil {
			t.Fatalf("len %d: Deserialize() error = %v", n, err)
		}
		assertPacketEqual(t, got, original)
	}
}

func TestPacketMaximumPayload(t *testing.T) {
	payload := make([]byte, 0xFFFF)
	for i := range payload {
		payload[i] = byte(i)
	}

	original := Packet{Type: Data, Seq: 1, Payload: payload}
	raw, err := original.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}
	if len(raw) != 4+len(payload)+4 {
		t.Fatalf("serialized length = %d, want %d", len(raw), 4+len(payload)+4)
	}

	var got Packet
	if err := got.Deserialize(raw); err != nil {
		t.Fatalf("Deserialize() error = %v", err)
	}
	assertPacketEqual(t, got, original)
}

func TestPacketSerializeLayout(t *testing.T) {
	payload := []byte{0x11, 0x22, 0x33}
	raw, err := (&Packet{Type: ACK, Seq: 0x5A, Payload: payload}).Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	if len(raw) != 4+len(payload)+4 {
		t.Fatalf("len = %d, want %d", len(raw), 4+len(payload)+4)
	}
	if raw[0] != byte(ACK) {
		t.Fatalf("type byte = %#x, want %#x", raw[0], byte(ACK))
	}
	if raw[1] != 0x5A {
		t.Fatalf("seq byte = %#x, want 0x5a", raw[1])
	}
	if got := binary.BigEndian.Uint16(raw[2:4]); got != uint16(len(payload)) {
		t.Fatalf("length field = %d, want %d", got, len(payload))
	}
	if !bytes.Equal(raw[4:4+len(payload)], payload) {
		t.Fatalf("payload = %x, want %x", raw[4:4+len(payload)], payload)
	}
}

func TestPacketSerializeDeterministic(t *testing.T) {
	first := Packet{Type: Data, Seq: 4, Payload: []byte("same")}
	second := Packet{Type: Data, Seq: 4, Payload: []byte("same")}

	raw1, err := first.Serialize()
	if err != nil {
		t.Fatalf("first Serialize() error = %v", err)
	}
	raw2, err := second.Serialize()
	if err != nil {
		t.Fatalf("second Serialize() error = %v", err)
	}
	if !bytes.Equal(raw1, raw2) {
		t.Fatalf("serializations differ: %x vs %x", raw1, raw2)
	}
}

func TestPacketPayloadIsolation(t *testing.T) {
	payload := []byte{1, 2, 3, 4}
	pkt := Packet{Type: Data, Seq: 1, Payload: payload}

	raw, err := pkt.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}
	payload[0] = 9
	if raw[4] != 1 {
		t.Fatal("Serialize aliased the caller's payload")
	}

	var got Packet
	if err := got.Deserialize(raw); err != nil {
		t.Fatalf("Deserialize() error = %v", err)
	}
	raw[4] = 8
	if got.Payload[0] != 1 {
		t.Fatal("Deserialize aliased the input buffer")
	}
	got.Payload[1] = 7
	if raw[5] != 2 {
		t.Fatal("packet payload aliases the input buffer")
	}
}

func TestPacketSerializeRejectsOversizedPayload(t *testing.T) {
	pkt := Packet{Type: Data, Seq: 1, Payload: make([]byte, 0xFFFF+1)}
	raw, err := pkt.Serialize()
	if err == nil {
		t.Fatal("Serialize() error = nil, want payload length error")
	}
	if err.Error() != "payload exceeds maximum length" {
		t.Fatalf("Serialize() error = %q", err.Error())
	}
	if raw != nil {
		t.Fatalf("Serialize() buffer = %x, want nil", raw)
	}
}

func TestPacketDeserializeErrors(t *testing.T) {
	valid, err := (&Packet{Type: Data, Seq: 1, Payload: []byte("abcdef")}).Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	declaredTooLong := bytes.Clone(valid)
	binary.BigEndian.PutUint16(declaredTooLong[2:4], uint16(len(valid)))

	cases := []struct {
		name string
		data []byte
		want string
	}{
		{name: "nil", data: nil, want: "too small for minimum packet"},
		{name: "empty", data: []byte{}, want: "too small for minimum packet"},
		{name: "header only", data: []byte{0, 1, 0, 0}, want: "too small for minimum packet"},
		{name: "seven bytes", data: []byte{0, 1, 0, 0, 1, 2, 3}, want: "too small for minimum packet"},
		{name: "declared payload exceeds buffer", data: declaredTooLong, want: "data too short for declared payload"},
		{
			name: "length claims one byte but buffer is header sized",
			data: []byte{byte(Data), 1, 0, 1, 0, 0, 0, 0},
			want: "data too short for declared payload",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got Packet
			err := got.Deserialize(tc.data)
			if err == nil {
				t.Fatal("Deserialize() error = nil")
			}
			if err.Error() != tc.want {
				t.Fatalf("Deserialize() error = %q, want %q", err.Error(), tc.want)
			}
		})
	}
}

func TestPacketChecksumDetectsCorruption(t *testing.T) {
	original := Packet{Type: Data, Seq: 0x3C, Payload: []byte("checksum-me")}
	raw, err := original.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	var got Packet
	if err := got.Deserialize(raw); err != nil {
		t.Fatalf("Deserialize(valid) error = %v", err)
	}

	corruptions := []struct {
		name  string
		index int
		want  string
	}{
		{name: "sequence", index: 1, want: "invalid checksum"},
		{name: "length", index: 3, want: "invalid checksum"},
		{name: "payload start", index: 4, want: "invalid checksum"},
		{name: "payload end", index: 4 + len(original.Payload) - 1, want: "invalid checksum"},
		{name: "checksum", index: len(raw) - 1, want: "invalid checksum"},
	}

	for _, tc := range corruptions {
		t.Run(tc.name, func(t *testing.T) {
			damaged := bytes.Clone(raw)
			damaged[tc.index] ^= 0x01

			var decoded Packet
			err := decoded.Deserialize(damaged)
			if err == nil {
				t.Fatal("Deserialize() error = nil, want corruption rejected")
			}
			if err.Error() != tc.want {
				t.Fatalf("Deserialize() error = %q, want %q", err.Error(), tc.want)
			}
		})
	}
}

func TestPacketDeserializeDoesNotModifyOnError(t *testing.T) {
	valid, err := (&Packet{Type: Data, Seq: 1, Payload: []byte("keep")}).Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}
	damaged := bytes.Clone(valid)
	damaged[len(damaged)-1] ^= 0xFF

	cases := []struct {
		name string
		data []byte
	}{
		{name: "too small", data: []byte{1, 2, 3}},
		{name: "short payload", data: []byte{byte(ACK), 9, 0, 5, 1, 2, 3, 4}},
		{name: "bad checksum", data: damaged},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Packet{Type: NAK, Seq: 42, Payload: []byte{7, 8}, Checksum: 99}
			if err := got.Deserialize(tc.data); err == nil {
				t.Fatal("Deserialize() error = nil")
			}
			assertPacketEqual(t, got, Packet{Type: NAK, Seq: 42, Payload: []byte{7, 8}, Checksum: 99})
		})
	}
}

func FuzzPacketRoundTrip(f *testing.F) {
	f.Add([]byte("hello"), byte(0), byte(Data))
	f.Add([]byte{}, byte(255), byte(NAK))
	f.Add([]byte{0x00, 0xff}, byte(1), byte(ACK))

	f.Fuzz(func(t *testing.T, payload []byte, seq byte, typ byte) {
		if len(payload) > 4096 {
			payload = payload[:4096]
		}

		original := Packet{Type: PacketType(typ), Seq: seq, Payload: payload}
		raw, err := original.Serialize()
		if err != nil {
			t.Fatalf("Serialize() error = %v", err)
		}

		var got Packet
		if err := got.Deserialize(raw); err != nil {
			t.Fatalf("Deserialize() error = %v", err)
		}
		assertPacketEqual(t, got, original)
	})
}

func assertPacketEqual(t *testing.T, got, want Packet) {
	t.Helper()
	if got.Type != want.Type || got.Seq != want.Seq || got.Checksum != want.Checksum || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("packet = {Type:%d Seq:%d Payload:%x Checksum:%08x}, want {Type:%d Seq:%d Payload:%x Checksum:%08x}",
			got.Type, got.Seq, got.Payload, got.Checksum,
			want.Type, want.Seq, want.Payload, want.Checksum)
	}
}
