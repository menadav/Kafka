package main

import (
	"fmt"
	"kafka/internal/protocol"
)

const (
	verbCreate = "create"
	verbList   = "list"
	verbInfo   = "info"
)

func buildRequest(args []string, replyPath string) (protocol.Request, error) {
	switch args[2] {
	case verbCreate:
		if len(args) < 4 {
			return protocol.Request{}, fmt.Errorf("create needs a topic name")
		}
		if !protocol.ValidName(args[3]) {
			return protocol.Request{}, fmt.Errorf("invalid topic name: %q", args[3])
		}
		return protocol.Request{
			Command:   protocol.CmdCreate,
			ReplyPath: replyPath,
			Topic:     args[3],
		}, nil
	case verbList:
		return protocol.Request{
			Command:   protocol.CmdList,
			ReplyPath: replyPath,
		}, nil
	case verbInfo:
		if len(args) < 4 {
			return protocol.Request{}, fmt.Errorf("info needs a client name")
		}
		if !protocol.ValidName(args[3]) {
			return protocol.Request{}, fmt.Errorf("invalid client name: %q", args[3])
		}
		return protocol.Request{
			Command:   protocol.CmdInfo,
			ReplyPath: replyPath,
			ClientID:  args[3],
		}, nil
	default:
		return protocol.Request{}, fmt.Errorf("unknown command: %q", args[2])
	}
}
