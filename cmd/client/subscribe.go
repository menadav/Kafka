package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strings"

	"kafka/internal/ipc"
	"kafka/internal/protocol"
)

func subscribe(s *session, req protocol.Request, serverPath string, raw bool) int {
	resp, err := s.roundTrip(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 3
	}
	if resp.Code != 0 {
		fmt.Fprintln(os.Stderr, resp.Body)
		return resp.Code
	}
	path := ipc.ChildPath(serverPath, req.ClientID)
	if err := ipc.CreateFIFO(path); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 3
	}
	s.setSubscriber(path, req.ClientID)
	channel, err := ipc.OpenReadNoWait(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 3
	}
	ready, err := s.roundTrip(protocol.Request{Command: protocol.CmdReady, ClientID: req.ClientID})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 3
	}
	if ready.Code != 0 {
		fmt.Fprintln(os.Stderr, ready.Body)
		return ready.Code
	}
	fmt.Println(resp.Body)
	lines := bufio.NewReader(channel)
	for {
		line, err := lines.ReadString('\n')
		if err != nil {
			if s.isLeaving() {
				return 0
			}
			fmt.Fprintln(os.Stderr, "server disconnected")
			return 3
		}
		if strings.TrimSuffix(line, "\n") == protocol.Goodbye {
			fmt.Fprintln(os.Stderr, "server shut down")
			return 0
		}
		offset, key, body, err := protocol.DecodeDelivery(line)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			continue
		}
		if raw {
			os.Stdout.Write(rawRecord(offset, key, body))
		} else {
			fmt.Printf("%s:%s\n", key, body)
		}
		ack, err := s.roundTrip(protocol.Request{
			Command:  protocol.CmdAck,
			ClientID: req.ClientID,
			Offset:   offset + 1,
		})
		if s.isLeaving() {
			return 0
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 3
		}
		if ack.Code != 0 {
			fmt.Fprintln(os.Stderr, ack.Body)
			return ack.Code
		}
	}
}

func rawRecord(offset uint32, key, value string) []byte {
	b := binary.LittleEndian.AppendUint32(nil, offset)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(key)))
	b = append(b, key...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(value)))
	return append(b, value...)
}
