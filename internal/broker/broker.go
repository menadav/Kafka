package broker

import(
    "kafka/internal/hashmap"
    "kafka/internal/trie"
    "slices"
)

func New() *Broker {
	return &Broker{
		topics: hashmap.New[*Topic](),
	}
}


func (b *Broker) Exists(name string) bool {
    _, answer := b.topics.Get(name) 
    return answer
}


func (b *Broker) Create(name string) bool {
    if b.Exists(name) {
        return false
    }
    elTopic := &Topic{
        name: name,
        subscribers: trie.New(),
    }
    b.topics.Put(name, elTopic)
    return true
}


func (b *Broker) List() []string {
    keys := b.topics.Keys()
    slices.Sort(keys)
    return keys
}