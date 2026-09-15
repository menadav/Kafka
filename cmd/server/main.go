package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"syscall"
	"kafka/internal/ipc"
)

func main() {
	path := ipc.BuildIPCPath(os.Getpid())

	err := syscall.Mkfifo(path, 0666)
	if err != nil {
		if os.IsExist(err) {
			os.Remove(path)
			err = syscall.Mkfifo(path, 0666)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	endpoint, err := os.OpenFile(path, os.O_RDWR, 0)
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
			reply, err := os.OpenFile(replyPath, os.O_WRONLY, 0)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				continue
			}
			reply.WriteString("PONG\n")
			reply.Close()
		}
	}
}
