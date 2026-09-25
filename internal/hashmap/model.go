package hashmap

type ClientMeta struct {
    ClientID string
    Topic    string
    Offset   uint32
    Prefix   string
    IPCPath  string
}

type node struct {
    key string
    meta ClientMeta
    next *node
}

type ClientIndex struct {
    buckets []*node
    size    int
}