package hashmap

const initialBuckets = 16

func New() *ClientIndex {
    return &ClientIndex{
        buckets: make([]*node, initialBuckets),
        size: 0,
        }
}

func hash(s string) uint32 {
    var h uint32 = 2166136261     // "offset basis" — el valor de arranque
    for i := 0; i < len(s); i++ {
        h ^= uint32(s[i])         // XOR con el byte actual
        h *= 16777619             // multiplicar por el "FNV prime"
    }
    return h
}


func (c *ClientIndex) Put(id string, meta ClientMeta) {
    actualBucket := hash(id) % uint32(len(c.buckets))
    for n := c.buckets[actualBucket]; n != nil; n = n.next {
        if n.key == id {
            n.meta = meta
            return            
        }
    }
    c.buckets[actualBucket] = &node{
        key: id,
        meta: meta,
        next: c.buckets[actualBucket],
    }
    c.size++
}

func (c *ClientIndex) Get(id string) (ClientMeta, bool) {
    actualBucket := hash(id) % uint32(len(c.buckets))
    for n := c.buckets[actualBucket]; n != nil; n = n.next {
        if n.key == id {
            return n.meta, true
        }
    }
    return ClientMeta{}, false
}

func (c *ClientIndex) Delete(id string){
    actualBucket := hash(id) % uint32(len(c.buckets))
    
    var prev *node
    for n := c.buckets[actualBucket]; n != nil; n = n.next {
        if n.key == id {
            if prev == nil{
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