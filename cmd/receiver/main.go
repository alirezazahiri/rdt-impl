// Command receiver is the rdt 2.1 receiver.
//
// It reads UDP datagrams, checks the checksum outside the state machine,
// and delivers each new in-order payload to stdout. A corrupt data packet
// is answered with NAK. A duplicate is answered with ACK and is not
// delivered again. ACK and NAK packets pass through an unreliable channel
// that may flip one bit before they are sent back to the sender.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/alirezazahiri/rdt/internal/channel"
	"github.com/alirezazahiri/rdt/internal/protocol"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("receiver: ")

	listen := flag.String("listen", ":9000", "UDP address to listen on")
	corrupt := flag.Float64("corrupt", 0, "chance in [0,1] of flipping one bit in an ACK or NAK")
	seed := flag.Int64("seed", 1, "seed for ACK and NAK corruption")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: receiver [flags]\n\n")
		fmt.Fprintf(os.Stderr, "Receives rdt 2.1 messages and prints each delivered payload.\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	addr, err := net.ResolveUDPAddr("udp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	link, err := channel.NewUnreliable(channel.Config{
		Seed:               *seed,
		CorruptProbability: *corrupt,
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("listening on %s corrupt=%.2f seed=%d", conn.LocalAddr(), *corrupt, *seed)

	receiver := protocol.NewReceiver()
	buf := make([]byte, 64*1024)
	for {
		n, client, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Fatal(err)
		}

		event := eventFromDatagram(buf[:n])
		if event.Corrupt {
			log.Printf("from %s corrupt packet", client)
		} else {
			log.Printf("from %s %s", client, describe(event.Packet))
		}

		actions, err := receiver.Handle(event)
		if err != nil {
			log.Printf("protocol: %v", err)
			continue
		}
		if err := apply(conn, client, link, actions); err != nil {
			log.Fatal(err)
		}
		log.Printf("state %s", receiver.State())
	}
}

func apply(conn *net.UDPConn, client *net.UDPAddr, link *channel.Unreliable, actions []protocol.Action) error {
	delivered := false
	for _, action := range actions {
		switch action.Kind {
		case protocol.DeliverData:
			delivered = true
			log.Printf("deliver %q", action.Data)
			if _, err := os.Stdout.Write(action.Data); err != nil {
				return err
			}
			if _, err := os.Stdout.Write([]byte{'\n'}); err != nil {
				return err
			}
		case protocol.SendPacket:
			corrupted, err := emit(conn, client, link, action.Packet)
			if err != nil {
				return err
			}
			note := ""
			if action.Packet != nil && action.Packet.Type == protocol.ACK && !delivered {
				note = " (duplicate)"
			}
			log.Printf("send %s%s corrupted=%t", describe(action.Packet), note, corrupted)
		default:
			return fmt.Errorf("unexpected action %s", action.Kind)
		}
	}
	return nil
}

func emit(conn *net.UDPConn, client *net.UDPAddr, link *channel.Unreliable, pkt *protocol.Packet) (bool, error) {
	if pkt == nil {
		return false, fmt.Errorf("action has no packet")
	}
	raw, err := protocol.EncodePacket(pkt)
	if err != nil {
		return false, err
	}

	before := link.Corrupted()
	if err := link.Send(raw); err != nil {
		return false, err
	}
	datagram, err := link.Receive()
	if err != nil {
		return false, err
	}
	if _, err := conn.WriteToUDP(datagram, client); err != nil {
		return false, err
	}
	return link.Corrupted() > before, nil
}

func eventFromDatagram(data []byte) protocol.Event {
	pkt, err := protocol.DecodePacket(data)
	// receiver only accepts data packets.
	if err != nil || pkt.Type != protocol.Data {
		return protocol.Event{Kind: protocol.PacketFromBelow, Corrupt: true}
	}
	return protocol.Event{Kind: protocol.PacketFromBelow, Packet: pkt}
}

func describe(pkt *protocol.Packet) string {
	if pkt == nil {
		return "none"
	}
	switch pkt.Type {
	case protocol.Data:
		return fmt.Sprintf("DATA seq=%d bytes=%d", pkt.Seq, len(pkt.Payload))
	case protocol.ACK:
		return "ACK"
	case protocol.NAK:
		return "NAK"
	default:
		return fmt.Sprintf("type=%d seq=%d", pkt.Type, pkt.Seq)
	}
}
