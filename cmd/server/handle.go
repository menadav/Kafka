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
	case protocol.CmdProduce:
		if !protocol.ValidName(req.Topic) {
			return protocol.Response{Code: 1, Body: "invalid topic name"}
		}
		if !protocol.ValidName(req.Key) {
			return protocol.Response{Code: 1, Body: "invalid key"}
		}
		if strings.Contains(req.Value, "\n") {
			return protocol.Response{Code: 1, Body: "value cannot contain a newline"}
		}
		if b.Produce(req.Topic, req.Key, []byte(req.Value)) {
			return protocol.Response{Code: 0, Body: "message produced"}
		}
		return protocol.Response{Code: 2, Body: "topic not found"}
	default:
		return protocol.Response{Code: 1, Body: "unknown command"}
	}
}
