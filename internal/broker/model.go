package broker

import (
	"kafka/internal/hashmap"
	"kafka/internal/trie"
)

type Message struct {
	Key    string
	Value  []byte
	Offset uint32
}

type Topic struct {
	name        string
	messages    []Message
	subscribers *trie.Trie
}

type Broker struct {
	topics *hashmap.Index[*Topic]
}
