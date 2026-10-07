package broker

import (
	"errors"
	"slices"

	"kafka/internal/hashmap"
)

var (
	ErrTopicNotFound  = errors.New("topic not found")
	ErrClientBusy     = errors.New("client already connected")
	ErrClientNotFound = errors.New("client not found")
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
	b.topics.Put(name, newTopic(name, b.Disconnect))
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
	offset := uint32(len(t.messages))
	t.messages = append(t.messages, Message{
		Key:    key,
		Value:  value,
		Offset: offset,
	})
	t.mu.Unlock()
	t.notify()
	return true
}

func (b *Broker) Subscribe(topic, clientID, prefix, ipcPath string, offset uint32, hasOffset bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	t, ok := b.topics.Get(topic)
	if !ok {
		return ErrTopicNotFound
	}
	c, known := b.clients.Get(clientID)
	if known && c.Connected {
		return ErrClientBusy
	}
	if !known {
		c = &ClientMeta{ClientID: clientID}
	} else if old, ok := b.topics.Get(c.Topic); ok {
		old.mu.Lock()
		old.removeSubscriber(clientID, c.Prefix)
		old.mu.Unlock()
	}
	if hasOffset {
		c.Offset = offset
	}
	c.Topic = topic
	c.Prefix = prefix
	c.IPCPath = ipcPath
	c.Connected = true
	t.mu.Lock()
	t.addSubscriber(clientID, prefix)
	t.mu.Unlock()
	b.clients.Put(clientID, c)
	return nil
}

func (t *Topic) addSubscriber(clientID, prefix string) {
	if prefix == "" {
		t.all = append(t.all, clientID)
		return
	}
	t.subscribers.Subscribe(prefix, clientID)
}

func (t *Topic) removeSubscriber(clientID, prefix string) {
	if prefix == "" {
		if i := slices.Index(t.all, clientID); i >= 0 {
			t.all = slices.Delete(t.all, i, i+1)
		}
		return
	}
	t.subscribers.Unsubscribe(prefix, clientID)
}

func (b *Broker) Attach(clientID string, ch Channel) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	c, ok := b.clients.Get(clientID)
	if !ok || !c.Connected {
		return ErrClientNotFound
	}
	t, ok := b.topics.Get(c.Topic)
	if !ok {
		return ErrTopicNotFound
	}

	t.control <- control{attach: &subscriber{
		id:     clientID,
		prefix: c.Prefix,
		next:   c.Offset,
		ch:     ch,
	}}
	return nil
}

func (b *Broker) Disconnect(clientID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.clients.Get(clientID)
	if !ok || !c.Connected {
		return
	}
	c.Connected = false
	t, ok := b.topics.Get(c.Topic)
	if !ok {
		return
	}
	t.mu.Lock()
	t.removeSubscriber(clientID, c.Prefix)
	t.mu.Unlock()
	t.control <- control{detach: clientID}
}

func (b *Broker) Commit(clientID string, offset uint32) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.clients.Get(clientID)
	if !ok || !c.Connected {
		return ErrClientNotFound
	}
	c.Offset = offset
	return nil
}
