package main

import (
    "strings"
    "kafka/internal/broker"
	"kafka/internal/protocol"
)

func handle(b *broker.Broker, req protocol.Request) protocol.Response {
	var resp protocol.Response

	switch req.Command {
	case protocol.CmdCreate:
		if b.Create(req.Topic) {
			resp = protocol.Response{Code: 0, Body: "topic created"}
		} else {
			resp = protocol.Response{Code: 2, Body: "topic already exists"}
		}
	case protocol.CmdList:
		resp = protocol.Response{Code: 0, Body: strings.Join(b.List(), ",")}
	case protocol.CmdInfo:
		resp = protocol.Response{Code: 2, Body: "client not found"}
	default:
		resp = protocol.Response{Code: 1, Body: "unknown command"}
	}
    return resp
}