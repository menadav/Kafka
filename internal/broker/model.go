package broker

import (
	"sync"

	"kafka/internal/hashmap"
	"kafka/internal/trie"
)

type ClientMeta struct {
	ClientID  string
	Topic     string
	Offset    uint32
	Prefix    string
	IPCPath   string
	inbox     chan Message
	Connected bool
}

type Message struct {
	Key    string
	Value  []byte
	Offset uint32
}

type Topic struct {
	name string

	mu          sync.Mutex
	messages    []Message
	subscribers *trie.Trie
	all         []string
}

type Broker struct {
	mu      sync.RWMutex
	topics  *hashmap.Index[*Topic]
	clients *hashmap.Index[*ClientMeta]
}
