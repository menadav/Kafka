package protocol

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	CmdCreate    = "CREATE"
	CmdList      = "LIST"
	CmdInfo      = "INFO"
	CmdProduce   = "PRODUCE"
	CmdSubscribe = "SUBSCRIBE"
	CmdExists    = "EXISTS"
)

const MaxPayload = 1024

const MaxLine = 4096

func (r Request) Encode() (string, error) {
	var line string
	switch r.Command {
	case CmdList:
		line = fmt.Sprintf("%s %s\n", r.Command, r.ReplyPath)
	case CmdCreate:
		line = fmt.Sprintf("%s %s %s\n", r.Command, r.ReplyPath, r.Topic)
	case CmdInfo:
		line = fmt.Sprintf("%s %s %s\n", r.Command, r.ReplyPath, r.ClientID)
	case CmdExists:
		line = fmt.Sprintf("%s %s %s\n", r.Command, r.ReplyPath, r.Topic)
	case CmdProduce:
		line = fmt.Sprintf("%s %s %s %s:%s\n", r.Command, r.ReplyPath, r.Topic, r.Key, r.Value)
	case CmdSubscribe:
		line = fmt.Sprintf("%s %s %s %s\n", r.Command, r.ReplyPath, r.Topic, r.Prefix)
	default:
		return "", fmt.Errorf("unknown command: %q", r.Command)
	}
	if len(line) > MaxLine {
		return "", fmt.Errorf("request too large: %d bytes, max %d", len(line), MaxLine)
	}
	return line, nil
}

func DecodeRequest(line string) (Request, error) {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return Request{}, fmt.Errorf("empty request")
	}

	command := fields[0]
	switch command {
	case CmdList:
		if len(fields) != 2 {
			return Request{}, fmt.Errorf("%s expects 1 argument, got %d", command, len(fields)-1)
		}
		return Request{Command: command, ReplyPath: fields[1]}, nil
	case CmdCreate:
		if len(fields) != 3 {
			return Request{}, fmt.Errorf("%s expects 2 arguments, got %d", command, len(fields)-1)
		}
		return Request{Command: command, ReplyPath: fields[1], Topic: fields[2]}, nil
	case CmdInfo:
		if len(fields) != 3 {
			return Request{}, fmt.Errorf("%s expects 2 arguments, got %d", command, len(fields)-1)
		}
		return Request{Command: command, ReplyPath: fields[1], ClientID: fields[2]}, nil
	case CmdExists:
		if len(fields) != 3 {
			return Request{}, fmt.Errorf("%s expects 2 arguments, got %d", command, len(fields)-1)
		}
		return Request{Command: command, ReplyPath: fields[1], Topic: fields[2]}, nil
	case CmdProduce:
		parts := strings.SplitN(strings.TrimSuffix(line, "\n"), " ", 4)
		if len(parts) != 4 {
			return Request{}, fmt.Errorf("%s expects 3 arguments, got %d", command, len(parts)-1)
		}
		key, value, ok := strings.Cut(parts[3], ":")
		if !ok {
			return Request{}, fmt.Errorf("%s message must be key:body", command)
		}
		return Request{Command: command, ReplyPath: parts[1], Topic: parts[2], Key: key, Value: value}, nil
	case CmdSubscribe:
		if len(fields) < 3 || len(fields) > 4 {
			return Request{}, fmt.Errorf("%s expects 2 or 3 arguments, got %d", command, len(fields)-1)
		}
		prefix := ""
		if len(fields) == 4 {
			prefix = fields[3]
		}
		return Request{Command: command, ReplyPath: fields[1], Topic: fields[2], Prefix: prefix}, nil
	default:
		return Request{}, fmt.Errorf("unknown command: %q", command)
	}
}

func (r Response) Encode() string {
	return fmt.Sprintf("%d %s\n", r.Code, r.Body)
}

func DecodeResponse(line string) (Response, error) {
	line = strings.TrimSuffix(line, "\n")

	parts := strings.SplitN(line, " ", 2)
	if len(parts) != 2 {
		return Response{}, fmt.Errorf("malformed response: %q", line)
	}
	code, err := strconv.Atoi(parts[0])
	if err != nil {
		return Response{}, fmt.Errorf("response code is not a number: %q", parts[0])
	}
	return Response{Code: code, Body: parts[1]}, nil
}
