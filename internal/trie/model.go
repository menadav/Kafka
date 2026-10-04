package trie

type node struct {
	children map[byte]*node
	subs     []string
}

type Trie struct {
	root *node
}
