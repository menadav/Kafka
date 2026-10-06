package protocol

type Request struct {
	Command   string
	ReplyPath string
	Topic     string
	ClientID  string
	Prefix    string
	Offset    uint32
	HasOffset bool // false: no --offset given, the server decides where to start
	Key       string
	Value     string
}

type Response struct {
	Code int
	Body string
}
