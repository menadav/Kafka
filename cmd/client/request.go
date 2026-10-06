package main

import (
	"fmt"

	"kafka/internal/protocol"
)

const (
	verbCreate  = "create"
	verbList    = "list"
	verbInfo    = "info"
	verbProduce = "produce"
)

func buildRequest(args []string) (protocol.Request, error) {
	switch args[2] {
	case verbCreate:
		if len(args) < 4 {
			return protocol.Request{}, fmt.Errorf("create needs a topic name")
		}
		if !protocol.ValidName(args[3]) {
			return protocol.Request{}, fmt.Errorf("invalid topic name: %q", args[3])
		}
		return protocol.Request{
			Command: protocol.CmdCreate,
			Topic:   args[3],
		}, nil
	case verbList:
		return protocol.Request{
			Command: protocol.CmdList,
		}, nil
	case verbInfo:
		if len(args) < 4 {
			return protocol.Request{}, fmt.Errorf("info needs a client name")
		}
		if !protocol.ValidName(args[3]) {
			return protocol.Request{}, fmt.Errorf("invalid client name: %q", args[3])
		}
		return protocol.Request{
			Command:  protocol.CmdInfo,
			ClientID: args[3],
		}, nil
	case verbProduce:
		if len(args) < 4 {
			return protocol.Request{}, fmt.Errorf("produce needs a topic name")
		}
		if !protocol.ValidName(args[3]) {
			return protocol.Request{}, fmt.Errorf("invalid topic name: %q", args[3])
		}
		for _, opt := range args[4:] {
			if opt == "--raw" {
				return protocol.Request{}, fmt.Errorf("--raw is not supported yet")
			}
			return protocol.Request{}, fmt.Errorf("unknown option: %q", opt)
		}
		return protocol.Request{
			Command: protocol.CmdProduce,
			Topic:   args[3],
		}, nil
	default:
		return protocol.Request{}, fmt.Errorf("unknown command: %q", args[2])
	}
}
