package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"kafka/internal/ipc"
	"kafka/internal/protocol"
)

const replyTimeout = 5 * time.Second

type session struct {
	replyPath string
	endpoint  *os.File
	reply     *os.File
	replies   *bufio.Reader

	mu          sync.Mutex
	channelPath string
}

func openSession(serverPath, replyPath string) (*session, error) {
	if err := ipc.CheckEndpoint(serverPath); err != nil {
		return nil, fmt.Errorf("invalid IPC identifier: %w", err)
	}
	if err := ipc.CreateFIFO(replyPath); err != nil {
		return nil, err
	}
	reply, err := ipc.OpenEndpoint(replyPath)
	if err != nil {
		os.Remove(replyPath)
		return nil, err
	}
	endpoint, err := ipc.OpenWrite(serverPath)
	if err != nil {
		reply.Close()
		os.Remove(replyPath)
		return nil, fmt.Errorf("cannot connect to %s: %w", serverPath, err)
	}
	return &session{
		replyPath: replyPath,
		endpoint:  endpoint,
		reply:     reply,
		replies:   bufio.NewReader(reply),
	}, nil
}

func (s *session) roundTrip(req protocol.Request) (protocol.Response, error) {
	req.ReplyPath = s.replyPath
	line, err := req.Encode()
	if err != nil {
		return protocol.Response{}, err
	}
	if _, err := s.endpoint.WriteString(line); err != nil {
		return protocol.Response{}, fmt.Errorf("server disconnected: %w", err)
	}
	s.reply.SetReadDeadline(time.Now().Add(replyTimeout))
	text, err := s.replies.ReadString('\n')
	if err != nil {
		return protocol.Response{}, fmt.Errorf("no reply from server: %w", err)
	}
	return protocol.DecodeResponse(text)
}

func (s *session) close() {
	s.endpoint.Close()
	s.reply.Close()
	os.Remove(s.replyPath)
	s.mu.Lock()
	if s.channelPath != "" {
		os.Remove(s.channelPath)
	}
	s.mu.Unlock()
}

func (s *session) setChannel(path string) {
	s.mu.Lock()
	s.channelPath = path
	s.mu.Unlock()
}

func (s *session) closeOnSignal() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signals
		s.close()
		os.Exit(0)
	}()
}
