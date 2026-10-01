package main

import (
	"bufio"
	"fmt"
	"os"
	"kafka/internal/broker"
	"kafka/internal/ipc"
	"kafka/internal/protocol"
)

func main() {
	path := ipc.BuildIPCPath(os.Getpid())
	err := ipc.CreateFIFO(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	endpoint, err := ipc.OpenEndpoint(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(path)
	reader := bufio.NewReader(endpoint)
	b := broker.New()
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			continue
		}
		req, err := protocol.DecodeRequest(line)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			continue
		}
		resp := handle(b, req)
		reply, err := ipc.OpenWrite(req.ReplyPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			continue
		}
		reply.WriteString(resp.Encode())
		reply.Close()
	}
}