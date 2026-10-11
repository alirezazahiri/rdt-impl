// Command sender is the rdt 2.1 sender.
//
// Each argument, or each non-empty line from stdin, is one application
// message. Outbound data packets pass through an unreliable channel that
// may flip one bit, then they are written as UDP datagrams. The process
// stays in the wait-for-ACK-or-NAK state until a checksum-valid ACK
// arrives, retransmitting after a NAK or a corrupt reply.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/alirezazahiri/rdt/internal/channel"
	"github.com/alirezazahiri/rdt/internal/protocol"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("sender: ")

	addr := flag.String("addr", "127.0.0.1:9000", "receiver UDP address")
	corrupt := flag.Float64("corrupt", 0, "chance in [0,1] of flipping one bit in an outbound data packet")
	seed := flag.Int64("seed", 1, "seed for data-packet corruption")
	timeout := flag.Duration("timeout", 5*time.Second, "how long to wait for one ACK or NAK")
	max := flag.Int("max", 32, "transmission cap per message, including the first send; 0 means no cap")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: sender [flags] [message ...]\n\n")
		fmt.Fprintf(os.Stderr, "Sends each message with rdt 2.1. With no messages, reads lines from stdin.\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *timeout <= 0 {
		log.Fatal("timeout must be positive")
	}

	link, err := channel.NewUnreliable(channel.Config{
		Seed:               *seed,
		CorruptProbability: *corrupt,
	})
	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.Dial("udp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	udp, ok := conn.(*net.UDPConn)
	if !ok {
		log.Fatal("dial did not return a UDP connection")
	}

	log.Printf("receiver %s corrupt=%.2f seed=%d timeout=%s max=%d", udp.RemoteAddr(), *corrupt, *seed, *timeout, *max)

	sender := protocol.NewSender()
	send := func(message string) error {
		return transfer(udp, link, sender, []byte(message), *timeout, *max)
	}
	if err := eachMessage(send); err != nil {
		log.Fatal(err)
	}
}

func eachMessage(send func(string) error) error {
	if args := flag.Args(); len(args) > 0 {
		for _, message := range args {
			if err := send(message); err != nil {
				return err
			}
		}
		return nil
	}

	log.Printf("reading messages from stdin")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		if err := send(line); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func transfer(conn *net.UDPConn, link *channel.Unreliable, sender *protocol.Sender, data []byte, timeout time.Duration, max int) error {
	log.Printf("rdt_send %q", data)
	actions, err := sender.Handle(protocol.Event{Kind: protocol.CallFromAbove, Data: data})
	if err != nil {
		return err
	}

	sent := 0
	before := link.Corrupted()
	if err := perform(conn, link, actions, &sent, max); err != nil {
		return err
	}
	log.Printf("state %s", sender.State())

	for waiting(sender.State()) {
		event, err := readReply(conn, timeout)
		if err != nil {
			return err
		}
		logReply(event)

		actions, err = sender.Handle(event)
		if err != nil {
			return err
		}
		if err := perform(conn, link, actions, &sent, max); err != nil {
			return err
		}
		log.Printf("state %s", sender.State())
	}

	log.Printf("acknowledged %q transmissions=%d corrupted=%d", data, sent, link.Corrupted()-before)
	return nil
}

func waiting(state protocol.SenderState) bool {
	return state == protocol.WaitForACKOrNAK0 || state == protocol.WaitForACKOrNAK1
}

func perform(conn *net.UDPConn, link *channel.Unreliable, actions []protocol.Action, sent *int, max int) error {
	for _, action := range actions {
		switch action.Kind {
		case protocol.SendPacket, protocol.Retransmit:
			*sent++
			if max > 0 && *sent > max {
				return fmt.Errorf("retransmission cap of %d reached", max)
			}
			corrupted, err := emit(conn, link, action.Packet)
			if err != nil {
				return err
			}
			log.Printf("%s %s corrupted=%t", action.Kind, describe(action.Packet), corrupted)
		default:
			return fmt.Errorf("unexpected action %s", action.Kind)
		}
	}
	return nil
}

func emit(conn *net.UDPConn, link *channel.Unreliable, pkt *protocol.Packet) (bool, error) {
	if pkt == nil {
		return false, errors.New("action has no packet")
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
	if _, err := conn.Write(datagram); err != nil {
		return false, err
	}
	return link.Corrupted() > before, nil
}

func readReply(conn *net.UDPConn, timeout time.Duration) (protocol.Event, error) {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return protocol.Event{}, err
	}

	buf := make([]byte, 64*1024)
	n, err := conn.Read(buf)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return protocol.Event{}, errors.New("timed out waiting for ACK or NAK; start the receiver first")
		}
		return protocol.Event{}, err
	}
	return eventFromDatagram(buf[:n]), nil
}

func logReply(event protocol.Event) {
	if event.Corrupt {
		log.Printf("reply corrupt")
		return
	}
	log.Printf("reply %s", describe(event.Packet))
}

func eventFromDatagram(data []byte) protocol.Event {
	pkt, err := protocol.DecodePacket(data)
	// anything other than ACK or NAK is unusable feedback.
	if err != nil || (pkt.Type != protocol.ACK && pkt.Type != protocol.NAK) {
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
