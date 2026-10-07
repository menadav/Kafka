package main

import (
	"bufio"
	"fmt"
	"os"

	"kafka/internal/ipc"
	"kafka/internal/protocol"
)

func subscribe(s *session, req protocol.Request, serverPath string) int {
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
	s.setChannel(path)
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
			fmt.Fprintln(os.Stderr, "server disconnected")
			return 3
		}
		offset, key, body, err := protocol.DecodeDelivery(line)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			continue
		}
		fmt.Printf("%s:%s\n", key, body)
		ack, err := s.roundTrip(protocol.Request{
			Command:  protocol.CmdAck,
			ClientID: req.ClientID,
			Offset:   offset + 1,
		})
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
