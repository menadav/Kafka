package main

import (
	"fmt"
	"log"
	"os"

	"kafka/internal/protocol"
)

var logger = log.New(os.Stderr, "", log.Ltime)

func logRequest(req protocol.Request, resp protocol.Response) {
	switch req.Command {
	case protocol.CmdAck:
		return
	case protocol.CmdProduce:
		if resp.Code == 0 {
			return
		}
	}
	logger.Printf("%-10s %-32s -> %d %s", req.Command, describe(req), resp.Code, resp.Body)
}

func describe(req protocol.Request) string {
	switch req.Command {
	case protocol.CmdCreate, protocol.CmdExists:
		return "topic=" + req.Topic
	case protocol.CmdProduce:
		return fmt.Sprintf("topic=%s key=%q", req.Topic, req.Key)
	case protocol.CmdInfo, protocol.CmdReady, protocol.CmdDisconnect:
		return "client=" + req.ClientID
	case protocol.CmdSubscribe:
		s := fmt.Sprintf("client=%s topic=%s", req.ClientID, req.Topic)
		if req.Prefix != "" {
			s += fmt.Sprintf(" prefix=%q", req.Prefix)
		}
		if req.HasOffset {
			s += fmt.Sprintf(" offset=%d", req.Offset)
		}
		return s
	}
	return ""
}
