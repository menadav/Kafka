package main

import (
	"fmt"
	"strconv"
	"strings"

	"kafka/internal/protocol"
)

const (
	verbCreate    = "create"
	verbList      = "list"
	verbInfo      = "info"
	verbProduce   = "produce"
	verbSubscribe = "subscribe"
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
	case verbSubscribe:
		if len(args) < 5 {
			return protocol.Request{}, fmt.Errorf("subscribe needs a topic name and a subscriber name")
		}
		if !protocol.ValidName(args[3]) {
			return protocol.Request{}, fmt.Errorf("invalid topic name: %q", args[3])
		}
		if !protocol.ValidName(args[4]) {
			return protocol.Request{}, fmt.Errorf("invalid client name: %q", args[4])
		}
		req := protocol.Request{
			Command:  protocol.CmdSubscribe,
			Topic:    args[3],
			ClientID: args[4],
		}
		for i := 5; i < len(args); i++ {
			switch args[i] {
			case "--prefix":
				value, err := optionValue(args, i)
				if err != nil {
					return protocol.Request{}, err
				}
				req.Prefix = value
				i++
			case "--offset":
				value, err := optionValue(args, i)
				if err != nil {
					return protocol.Request{}, err
				}
				n, err := strconv.ParseUint(value, 10, 32)
				if err != nil {
					return protocol.Request{}, fmt.Errorf("invalid offset: %q", value)
				}
				req.Offset = uint32(n)
				req.HasOffset = true
				i++
			case "--raw":
				return protocol.Request{}, fmt.Errorf("--raw is not supported yet")
			default:
				return protocol.Request{}, fmt.Errorf("unknown option: %q", args[i])
			}
		}
		return req, nil
	default:
		return protocol.Request{}, fmt.Errorf("unknown command: %q", args[2])
	}
}

func optionValue(args []string, i int) (string, error) {
	if i+1 >= len(args) || strings.HasPrefix(args[i+1], "--") {
		return "", fmt.Errorf("%s needs a value", args[i])
	}
	return args[i+1], nil
}
