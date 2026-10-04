package hashmap

const initialBuckets = 16

func New[T any]() *Index[T] {
	return &Index[T]{
		buckets: make([]*node[T], initialBuckets),
		size:    0,
	}
}

func hash(s string) uint32 {
	var h uint32 = 2166136261 // "offset basis" — el valor de arranque
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i]) // XOR con el byte actual
		h *= 16777619     // multiplicar por el "FNV prime"
	}
	return h
}

func (c *Index[T]) Put(id string, meta T) {
	actualBucket := hash(id) % uint32(len(c.buckets))
	for n := c.buckets[actualBucket]; n != nil; n = n.next {
		if n.key == id {
			n.meta = meta
			return
		}
	}
	c.buckets[actualBucket] = &node[T]{
		key:  id,
		meta: meta,
		next: c.buckets[actualBucket],
	}
	c.size++
}

func (c *Index[T]) Get(id string) (T, bool) {
	actualBucket := hash(id) % uint32(len(c.buckets))
	for n := c.buckets[actualBucket]; n != nil; n = n.next {
		if n.key == id {
			return n.meta, true
		}
	}
	var zero T
	return zero, false
}

func (c *Index[T]) Delete(id string) {
	actualBucket := hash(id) % uint32(len(c.buckets))

	var prev *node[T]
	for n := c.buckets[actualBucket]; n != nil; n = n.next {
		if n.key == id {
			if prev == nil {
				c.buckets[actualBucket] = n.next
			} else {
				prev.next = n.next
			}
			c.size--
			return
		}
		prev = n
	}
}

func (c *Index[T]) Keys() []string {
	keys := make([]string, 0, c.size)
	for _, bucket := range c.buckets {
		for n := bucket; n != nil; n = n.next {
			keys = append(keys, n.key)
		}
	}
	return keys
}
