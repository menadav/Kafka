package broker

import (
	"slices"
	"strings"

	"kafka/internal/hashmap"
	"kafka/internal/trie"
)

type Channel interface {
	Send(m Message) error
	Close() error
}

type subscriber struct {
	id     string
	prefix string
	next   uint32
	ch     Channel
}

type control struct {
	attach *subscriber
	detach string
}

type delivery struct {
	msg Message
	ids []string
}

func newTopic(name string, onGone func(clientID string)) *Topic {
	t := &Topic{
		name:        name,
		subscribers: trie.New(),
		control:     make(chan control, 16),
		wake:        make(chan struct{}, 1),
		onGone:      onGone,
	}
	go t.run()
	return t
}

func (t *Topic) notify() {
	select {
	case t.wake <- struct{}{}:
	default:
	}
}

func (t *Topic) run() {
	active := hashmap.New[*subscriber]()
	var routed uint32
	for {
		select {
		case c := <-t.control:
			if c.attach != nil {
				if t.catchUp(c.attach, routed) {
					active.Put(c.attach.id, c.attach)
				}
			} else if s, ok := active.Get(c.detach); ok {
				s.ch.Close()
				active.Delete(c.detach)
			}
		case <-t.wake:
		}

		var batch []delivery
		batch, routed = t.route(routed)
		for _, d := range batch {
			for _, id := range d.ids {
				s, ok := active.Get(id)
				if !ok || d.msg.Offset < s.next {
					continue
				}
				if !t.send(s, d.msg) {
					active.Delete(id)
				}
			}
		}
	}
}

func (t *Topic) route(routed uint32) ([]delivery, uint32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var batch []delivery
	for _, m := range t.messages[routed:] {
		ids := append(slices.Clone(t.all), t.subscribers.Match(m.Key)...)
		batch = append(batch, delivery{msg: m, ids: ids})
	}
	return batch, uint32(len(t.messages))
}

func (t *Topic) catchUp(s *subscriber, routed uint32) bool {
	if s.next < routed {
		t.mu.Lock()
		history := t.messages[s.next:routed]
		t.mu.Unlock()
		for _, m := range history {
			if strings.HasPrefix(m.Key, s.prefix) && !t.send(s, m) {
				return false
			}
		}
	}
	s.next = max(s.next, routed)
	return true
}

func (t *Topic) send(s *subscriber, m Message) bool {
	if err := s.ch.Send(m); err != nil {
		s.ch.Close()
		go t.onGone(s.id)
		return false
	}
	s.next = m.Offset + 1
	return true
}
