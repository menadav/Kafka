package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"kafka/internal/broker"
	"kafka/internal/ipc"
	"kafka/internal/protocol"
)

const shutdownTimeout = 5 * time.Second

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
	logger.Printf("listening on %s", path)
	reader := bufio.NewReader(endpoint)
	b := broker.New()
	b.SetLogger(logger.Printf)
	go serve(path, reader, b)
	<-ctx.Done()
	stop()
	logger.Printf("shutting down: waiting up to %s for consumers to commit", shutdownTimeout)
	os.Remove(path)
	start := time.Now()
	b.Shutdown(shutdownTimeout)
	logger.Printf("consumers told goodbye after %s", time.Since(start).Round(time.Millisecond))
	logger.Printf("removed %d leftover FIFOs, bye", removeFIFOs(path))
}

func removeFIFOs(path string) int {
	removed := 0
	matches, _ := filepath.Glob(path + ".*")
	for _, m := range matches {
		info, err := os.Lstat(m)
		if err == nil && info.Mode()&os.ModeNamedPipe != 0 && os.Remove(m) == nil {
			removed++
		}
	}
	return removed
}

func serve(path string, reader *bufio.Reader, b *broker.Broker) {
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			continue
		}
		req, err := protocol.DecodeRequest(line)
		if err != nil {
			logger.Printf("bad request: %v", err)
			continue
		}
		if !ownReplyPath(path, req.ReplyPath) {
			logger.Printf("refusing reply path %q", req.ReplyPath)
			continue
		}
		resp := handle(b, path, req)
		logRequest(req, resp)
		reply, err := ipc.OpenWrite(req.ReplyPath)
		if err != nil {
			logger.Printf("cannot answer %s: %v", req.ReplyPath, err)
			continue
		}
		reply.WriteString(resp.Encode())
		reply.Close()
	}
}

func ownReplyPath(path, reply string) bool {
	suffix, ok := strings.CutPrefix(reply, path+".")
	return ok && suffix != "" && !strings.Contains(suffix, "/")
}
