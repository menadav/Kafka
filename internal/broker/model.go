package broker

import (
	"sync"

	"kafka/internal/hashmap"
	"kafka/internal/trie"
)

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
}

type Broker struct {
	mu     sync.RWMutex
	topics *hashmap.Index[*Topic]
}
