package hashmap

type node[T any] struct {
	key  string
	meta T
	next *node[T]
}

type Index[T any] struct {
	buckets []*node[T]
	size    int
}
