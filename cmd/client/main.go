package main

import (
	"fmt"
	"os"
	"strconv"

	"kafka/internal/ipc"
	"kafka/internal/protocol"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: client <ipc_identifier> <command> [args]")
		os.Exit(1)
	}
	serverPath := os.Args[1]
	replyPath := ipc.ChildPath(serverPath, strconv.Itoa(os.Getpid()))

	req, err := buildRequest(os.Args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	s, err := openSession(serverPath, replyPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	s.closeOnSignal()

	var code int
	switch req.Command {
	case protocol.CmdProduce:
		code = produce(s, req.Topic, os.Stdin)
	case protocol.CmdSubscribe:
		code = subscribe(s, req, serverPath)
	default:
		code = send(s, req)
	}
	s.close()
	os.Exit(code)
}

func send(s *session, req protocol.Request) int {
	resp, err := s.roundTrip(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 3
	}
	if resp.Code != 0 {
		fmt.Fprintln(os.Stderr, resp.Body)
		return resp.Code
	}
	fmt.Println(resp.Body)
	return 0
}
