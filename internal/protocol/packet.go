package protocol

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

type PacketType uint8

const (
	Data PacketType = iota
	ACK
	NAK
)

type Packet struct {
	Type     PacketType
	Seq      uint8
	Payload  []byte
	Checksum uint32
}

func (p *Packet) Serialize() ([]byte, error) {
	if len(p.Payload) > 0xFFFF {
		return nil, errors.New("payload exceeds maximum length")
	}

	// packet format: type(1) + seq(1) + payload-length(2) + payload(N) + checksum(4)
	totalLength := 4 + len(p.Payload) + 4
	buf := make([]byte, totalLength)

	buf[0] = byte(p.Type)
	buf[1] = p.Seq
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(p.Payload)))
	copy(buf[4:], p.Payload)

	// checksum covers every byte after the type. The checksum field is still zero here.
	checksum, err := p.calculateChecksum(buf[1:])
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint32(buf[totalLength-4:], checksum)
	p.Checksum = checksum

	return buf, nil
}

func (p *Packet) Deserialize(data []byte) error {
	if len(data) < 8 {
		return errors.New("too small for minimum packet")
	}

	payloadLength := binary.BigEndian.Uint16(data[2:4])
	packetLen := 4 + int(payloadLength) + 4
	if len(data) < packetLen {
		return errors.New("data too short for declared payload")
	}

	covered := make([]byte, packetLen-1)
	copy(covered, data[1:packetLen])
	for i := len(covered) - 4; i < len(covered); i++ {
		covered[i] = 0
	}

	receivedChecksum := binary.BigEndian.Uint32(data[packetLen-4 : packetLen])
	calculatedChecksum, err := p.calculateChecksum(covered)
	if err != nil {
		return err
	}
	if receivedChecksum != calculatedChecksum {
		return errors.New("invalid checksum")
	}

	payload := make([]byte, int(payloadLength))
	copy(payload, data[4:4+int(payloadLength)])

	p.Type = PacketType(data[0])
	p.Seq = data[1]
	p.Payload = payload
	p.Checksum = receivedChecksum
	return nil
}

func (p *Packet) calculateChecksum(data []byte) (uint32, error) {
	crc := crc32.NewIEEE()
	if _, err := crc.Write(data); err != nil {
		return 0, err
	}
	return crc.Sum32(), nil
}
