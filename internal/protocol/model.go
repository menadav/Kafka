package protocol

type Request struct {
	Command   string
	ReplyPath string
	Topic     string
	ClientID  string
	Prefix    string
	Offset    uint32
	HasOffset bool
	Key       string
	Value     string
}

type Response struct {
	Code int
	Body string
}
