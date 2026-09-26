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

