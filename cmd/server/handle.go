package main

import (
	"strings"

	"kafka/internal/broker"
	"kafka/internal/protocol"
)

func handle(b *broker.Broker, req protocol.Request) protocol.Response {
	switch req.Command {
	case protocol.CmdCreate:
		if !protocol.ValidName(req.Topic) {
			return protocol.Response{Code: 1, Body: "invalid topic name"}
		}
		if b.Create(req.Topic) {
			return protocol.Response{Code: 0, Body: "topic created"}
		}
		return protocol.Response{Code: 2, Body: "topic already exists"}

	case protocol.CmdList:
		return protocol.Response{Code: 0, Body: strings.Join(b.List(), ",")}

	case protocol.CmdInfo:
		return protocol.Response{Code: 2, Body: "client not found"}

	default:
		return protocol.Response{Code: 1, Body: "unknown command"}
	}
}
