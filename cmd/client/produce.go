package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"kafka/internal/protocol"
)

func produce(s *session, topic string, in io.Reader, raw bool) int {
	resp, err := s.roundTrip(protocol.Request{Command: protocol.CmdExists, Topic: topic})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 3
	}
	if resp.Code != 0 {
		fmt.Fprintln(os.Stderr, resp.Body)
		return resp.Code
	}

	var messages messageReader = &textReader{scanner: bufio.NewScanner(in)}
	if raw {
		messages = &rawReader{r: bufio.NewReader(in)}
	}
	for {
		key, body, err := messages.next()
		if err == io.EOF {
			return 0
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
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
}

type messageReader interface {
	next() (key, body string, err error)
}

type textReader struct {
	scanner *bufio.Scanner
}

func (t *textReader) next() (string, string, error) {
	for t.scanner.Scan() {
		line := t.scanner.Text()
		if line == "" {
			continue
		}
		key, body, ok := strings.Cut(line, ":")
		if !ok {
			return "", "", fmt.Errorf("invalid message %q: expected key:body", line)
		}
		return key, body, nil
	}
	if err := t.scanner.Err(); err != nil {
		return "", "", err
	}
	return "", "", io.EOF
}

var errPartialRecord = errors.New("partial record at end of input")

type rawReader struct {
	r *bufio.Reader
}

func (r *rawReader) next() (string, string, error) {
	key, err := r.field()
	if err == io.EOF {
		return "", "", io.EOF
	}
	if err != nil {
		return "", "", err
	}
	value, err := r.field()
	if err == io.EOF {
		return "", "", errPartialRecord
	}
	if err != nil {
		return "", "", err
	}
	return key, value, nil
}

func (r *rawReader) field() (string, error) {
	var size [4]byte
	if _, err := io.ReadFull(r.r, size[:]); err != nil {
		if err == io.EOF {
			return "", io.EOF
		}
		return "", errPartialRecord
	}
	n := binary.LittleEndian.Uint32(size[:])
	if n > protocol.MaxPayload {
		return "", fmt.Errorf("field too large: %d bytes, max %d", n, protocol.MaxPayload)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return "", errPartialRecord
	}
	return string(buf), nil
}
