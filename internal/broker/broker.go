package broker

import (
	"slices"

	"kafka/internal/hashmap"
	"kafka/internal/trie"
)

func New() *Broker {
	return &Broker{
		topics:  hashmap.New[*Topic](),
		clients: hashmap.New[*ClientMeta](),
	}
}

func (b *Broker) Exists(name string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.exists(name)
}

func (b *Broker) exists(name string) bool {
	_, answer := b.topics.Get(name)
	return answer
}

func (b *Broker) Create(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.exists(name) {
		return false
	}
	elTopic := &Topic{
		name:        name,
		subscribers: trie.New(),
	}
	b.topics.Put(name, elTopic)
	return true
}

func (b *Broker) List() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	keys := b.topics.Keys()
	slices.Sort(keys)
	return keys
}

func (b *Broker) Produce(name string, key string, value []byte) bool {
	b.mu.RLock()
	t, ok := b.topics.Get(name)
	b.mu.RUnlock()
	if !ok {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	offset := uint32(len(t.messages))
	t.messages = append(t.messages, Message{
		Key:    key,
		Value:  value,
		Offset: offset,
	})
	return true
}
