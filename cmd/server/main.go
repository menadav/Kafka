package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"kafka/internal/ipc"
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
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			continue
		}
		if strings.HasPrefix(line, "PING ") {
			replyPath := strings.TrimSpace(strings.TrimPrefix(line, "PING "))
			reply, err := ipc.OpenWrite(replyPath)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				continue
			}
			reply.WriteString("PONG\n")
			reply.Close()
		}
	}
}
