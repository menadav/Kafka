package trie

func newNode() *node {
    return &node{
        children: make(map[byte]*node),
        }
}

func New() *Trie {
    return &Trie{
        root: newNode(),
    }
}


func (t *Trie) Subscribe(prefix string, clientID string) {
    actual := t.root

    for _, letter := range []byte(prefix) {
        if actual.children[letter] == nil {
            actual.children[letter] = newNode()
        }
        actual = actual.children[letter]
    }
    actual.subs = append(actual.subs, clientID)
}

func (t *Trie) Match(key string) []string {
    actual := t.root
    result := []string{}

    result = append(result, actual.subs...)
    for _, letter := range []byte(key){
        actual = actual.children[letter]
        if actual == nil {
            return result
        }
        result = append(result, actual.subs...)
    }
    return result
}

func (t *Trie) Unsubscribe(prefix string, clientID string) {
    actual := t.root

    for _, letter := range []byte(prefix) {
        if actual.children[letter] == nil {
            return
        }
        actual = actual.children[letter]
    }

    for i := 0; i < len(actual.subs); i++ {
        if actual.subs[i] == clientID {
            actual.subs = append(actual.subs[:i], actual.subs[i+1:]...)
            return
        }
    }
}
