package protocol

func EncodePacket(packet *Packet) ([]byte, error) {
	return packet.Serialize()
}

func DecodePacket(data []byte) (*Packet, error) {
	packet := &Packet{}
	if err := packet.Deserialize(data); err != nil {
		return nil, err
	}
	return packet, nil
}
