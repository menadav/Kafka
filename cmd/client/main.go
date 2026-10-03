package main

import (
	"fmt"
	"io"
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

	req, err := buildRequest(os.Args, replyPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	message, err := req.Encode()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := ipc.CreateFIFO(replyPath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	endpoint, err := ipc.OpenWrite(serverPath)
	if err != nil {
		os.Remove(replyPath)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	endpoint.WriteString(message)
	endpoint.Close()
	reply, err := ipc.OpenRead(replyPath)
	if err != nil {
		os.Remove(replyPath)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	data, err := io.ReadAll(reply)
	reply.Close()
	os.Remove(replyPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	resp, err := protocol.DecodeResponse(string(data))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	if resp.Code != 0 {
		fmt.Fprintln(os.Stderr, resp.Body)
		os.Exit(resp.Code)
	}
	fmt.Println(resp.Body)
}
