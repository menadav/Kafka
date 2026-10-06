package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"kafka/internal/protocol"
)

func produce(s *session, topic string, in io.Reader) int {
	resp, err := s.roundTrip(protocol.Request{Command: protocol.CmdExists, Topic: topic})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 3
	}
	if resp.Code != 0 {
		fmt.Fprintln(os.Stderr, resp.Body)
		return resp.Code
	}

	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		key, body, ok := strings.Cut(line, ":")
		if !ok {
			fmt.Fprintf(os.Stderr, "invalid message %q: expected key:body\n", line)
			return 1
		}
		if len(key)+len(body) > protocol.MaxPayload {
			fmt.Fprintf(os.Stderr, "message too large: %d bytes, max %d\n", len(key)+len(body), protocol.MaxPayload)
			return 1
		}
		resp, err := s.roundTrip(protocol.Request{
			Command: protocol.CmdProduce,
			Topic:   topic,
			Key:     key,
			Value:   body,
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 3
		}
		if resp.Code != 0 {
			fmt.Fprintln(os.Stderr, resp.Body)
			return resp.Code
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}
