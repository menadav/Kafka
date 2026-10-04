package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"kafka/internal/broker"
	"kafka/internal/ipc"
	"kafka/internal/protocol"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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
	go serve(reader, b)
	<-ctx.Done()
	fmt.Fprintln(os.Stderr, "shutting down")
	os.Remove(path)
}

func serve(reader *bufio.Reader, b *broker.Broker) {
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
