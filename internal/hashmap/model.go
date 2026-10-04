package hashmap

type ClientMeta struct {
	ClientID string
	Topic    string
	Offset   uint32
	Prefix   string
	IPCPath  string
}

type node[T any] struct {
	key  string
	meta T
	next *node[T]
}

type Index[T any] struct {
	buckets []*node[T]
	size    int
}
