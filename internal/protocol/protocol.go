package protocol

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	CmdCreate  = "CREATE"
	CmdList    = "LIST"
	CmdInfo    = "INFO"
	CmdProduce = "PRODUCE"
)

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
	case CmdProduce:
		line = fmt.Sprintf("%s %s %s %s %s\n", r.Command, r.ReplyPath, r.Topic, r.Key, r.Value)
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
	case CmdProduce:
		parts := strings.SplitN(line, " ", 5)
		if len(parts) != 5 {
			return Request{}, fmt.Errorf("%s expects 4 arguments, got %d", command, len(parts)-1)
		}
		value := strings.TrimSuffix(parts[4], "\n")
		return Request{Command: command, ReplyPath: parts[1], Topic: parts[2], Key: parts[3], Value: value}, nil
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
