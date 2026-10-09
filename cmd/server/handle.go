package main

import (
	"errors"
	"strings"

	"kafka/internal/broker"
	"kafka/internal/ipc"
	"kafka/internal/protocol"
)

func handle(b *broker.Broker, serverPath string, req protocol.Request) protocol.Response {
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
	case protocol.CmdExists:
		if !protocol.ValidName(req.Topic) {
			return protocol.Response{Code: 1, Body: "invalid topic name"}
		}
		if !b.Exists(req.Topic) {
			return protocol.Response{Code: 2, Body: "topic not found"}
		}
		return protocol.Response{Code: 0, Body: "ok"}
	case protocol.CmdProduce:
		if !protocol.ValidName(req.Topic) {
			return protocol.Response{Code: 1, Body: "invalid topic name"}
		}
		if len(req.Key)+len(req.Value) > protocol.MaxPayload {
			return protocol.Response{Code: 1, Body: "message too large"}
		}
		if b.Produce(req.Topic, req.Key, []byte(req.Value)) {
			return protocol.Response{Code: 0, Body: "message produced"}
		}
		return protocol.Response{Code: 2, Body: "topic not found"}
	case protocol.CmdSubscribe:
		if !protocol.ValidName(req.Topic) {
			return protocol.Response{Code: 1, Body: "invalid topic name"}
		}
		if !protocol.ValidName(req.ClientID) {
			return protocol.Response{Code: 1, Body: "invalid client name"}
		}
		ipcPath := ipc.ChildPath(serverPath, req.ClientID)
		err := b.Subscribe(req.Topic, req.ClientID, req.Prefix, ipcPath, req.Offset, req.HasOffset)
		switch {
		case errors.Is(err, broker.ErrTopicNotFound):
			return protocol.Response{Code: 2, Body: "topic not found"}
		case errors.Is(err, broker.ErrClientBusy):
			return protocol.Response{Code: 2, Body: "client already connected"}
		case err != nil:
			return protocol.Response{Code: 1, Body: err.Error()}
		}
		return protocol.Response{Code: 0, Body: "subscribed to " + req.Topic}
	case protocol.CmdReady:

		if !protocol.ValidName(req.ClientID) {
			return protocol.Response{Code: 1, Body: "invalid client name"}
		}
		f, err := ipc.OpenWrite(ipc.ChildPath(serverPath, req.ClientID))
		if err != nil {
			b.Disconnect(req.ClientID)
			return protocol.Response{Code: 3, Body: "cannot open the consumer channel"}
		}
		if err := b.Attach(req.ClientID, consumerChannel{f: f}); err != nil {
			f.Close()
			return protocol.Response{Code: 2, Body: "client not found"}
		}
		return protocol.Response{Code: 0, Body: "ready"}
	case protocol.CmdAck:
		if !protocol.ValidName(req.ClientID) {
			return protocol.Response{Code: 1, Body: "invalid client name"}
		}
		if err := b.Commit(req.ClientID, req.Offset); err != nil {
			return protocol.Response{Code: 2, Body: "client not found"}
		}
		return protocol.Response{Code: 0, Body: "ok"}
	case protocol.CmdDisconnect:
		if !protocol.ValidName(req.ClientID) {
			return protocol.Response{Code: 1, Body: "invalid client name"}
		}
		b.Disconnect(req.ClientID)
		return protocol.Response{Code: 0, Body: "ok"}
	default:
		return protocol.Response{Code: 1, Body: "unknown command"}
	}
}
