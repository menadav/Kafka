package hashmap

const initialBuckets = 16

func New() *ClientIndex {
    make([]*node, initialBuckets)
    return &ClientIndex{buckets: ..., size: 0}
}