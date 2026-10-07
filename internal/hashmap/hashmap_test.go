package hashmap

import (
	"slices"
	"testing"
)

type ClientMeta struct {
	ClientID string
	Topic    string
	Offset   uint32
	Prefix   string
	IPCPath  string
}

const collideA = "client1"
const collideB = "moulinette"

func TestIndexWithClientMeta(t *testing.T) {
	t.Run("put then get", func(t *testing.T) {
		idx := New[ClientMeta]()
		idx.Put("client0", ClientMeta{ClientID: "client0", Topic: "user_events", Offset: 7})

		got, ok := idx.Get("client0")
		if !ok {
			t.Fatalf("Get(%q) reported not found, want found", "client0")
		}
		if got.Topic != "user_events" || got.Offset != 7 {
			t.Errorf("Get(%q) = %+v, want Topic=user_events Offset=7", "client0", got)
		}
	})

	t.Run("get of a key never stored", func(t *testing.T) {
		idx := New[ClientMeta]()

		got, ok := idx.Get("ghost")
		if ok {
			t.Errorf("Get(%q) reported found, want not found", "ghost")
		}
		if got != (ClientMeta{}) {
			t.Errorf("Get(%q) = %+v, want the zero value", "ghost", got)
		}
	})

	t.Run("put on an existing key overwrites and does not grow", func(t *testing.T) {
		idx := New[ClientMeta]()
		idx.Put("client0", ClientMeta{Topic: "old", Offset: 1})
		idx.Put("client0", ClientMeta{Topic: "new", Offset: 42})

		got, _ := idx.Get("client0")
		if got.Topic != "new" || got.Offset != 42 {
			t.Errorf("Get after overwrite = %+v, want Topic=new Offset=42", got)
		}
		if idx.size != 1 {
			t.Errorf("size = %d after overwriting the same key, want 1", idx.size)
		}
	})

	t.Run("colliding keys are both retrievable", func(t *testing.T) {
		idx := New[ClientMeta]()
		idx.Put(collideA, ClientMeta{Topic: "A"})
		idx.Put(collideB, ClientMeta{Topic: "B"})

		if hash(collideA)%initialBuckets != hash(collideB)%initialBuckets {
			t.Fatalf("%q and %q no longer collide; pick another pair for this test", collideA, collideB)
		}

		a, okA := idx.Get(collideA)
		b, okB := idx.Get(collideB)
		if !okA || a.Topic != "A" {
			t.Errorf("Get(%q) = %+v (found=%v), want Topic=A", collideA, a, okA)
		}
		if !okB || b.Topic != "B" {
			t.Errorf("Get(%q) = %+v (found=%v), want Topic=B", collideB, b, okB)
		}
	})
}

func TestIndexDelete(t *testing.T) {
	t.Run("deleting removes the entry", func(t *testing.T) {
		idx := New[ClientMeta]()
		idx.Put("client0", ClientMeta{Topic: "x"})

		idx.Delete("client0")

		if _, ok := idx.Get("client0"); ok {
			t.Errorf("Get(%q) after Delete reported found, want not found", "client0")
		}
		if idx.size != 0 {
			t.Errorf("size = %d after deleting the only entry, want 0", idx.size)
		}
	})

	t.Run("deleting one collided key keeps the other", func(t *testing.T) {
		idx := New[ClientMeta]()
		idx.Put(collideA, ClientMeta{Topic: "A"})
		idx.Put(collideB, ClientMeta{Topic: "B"})

		idx.Delete(collideA)

		if _, ok := idx.Get(collideA); ok {
			t.Errorf("Get(%q) after Delete reported found, want not found", collideA)
		}
		survivor, ok := idx.Get(collideB)
		if !ok || survivor.Topic != "B" {
			t.Errorf("Get(%q) = %+v (found=%v), want Topic=B — the chain was broken",
				collideB, survivor, ok)
		}
	})

	t.Run("deleting a key that is not there changes nothing", func(t *testing.T) {
		idx := New[ClientMeta]()
		idx.Put("client0", ClientMeta{Topic: "x"})

		idx.Delete("ghost")

		if _, ok := idx.Get("client0"); !ok {
			t.Errorf("Get(%q) after deleting an absent key reported not found", "client0")
		}
		if idx.size != 1 {
			t.Errorf("size = %d after deleting an absent key, want 1", idx.size)
		}
	})
}

type fakeTopic struct {
	Name string
}

func TestIndexIsGeneric(t *testing.T) {
	t.Run("holds a pointer type", func(t *testing.T) {
		idx := New[*fakeTopic]()
		idx.Put("user_events", &fakeTopic{Name: "user_events"})

		got, ok := idx.Get("user_events")
		if !ok {
			t.Fatalf("Get(%q) reported not found, want found", "user_events")
		}
		if got.Name != "user_events" {
			t.Errorf("Get(%q).Name = %q, want %q", "user_events", got.Name, "user_events")
		}
	})

	t.Run("zero value of a pointer type is nil", func(t *testing.T) {
		idx := New[*fakeTopic]()

		got, ok := idx.Get("missing")
		if ok {
			t.Errorf("Get(%q) reported found, want not found", "missing")
		}
		if got != nil {
			t.Errorf("Get(%q) = %v, want nil", "missing", got)
		}
	})

	t.Run("holds a plain int", func(t *testing.T) {
		idx := New[int]()
		idx.Put("answer", 42)

		got, ok := idx.Get("answer")
		if !ok || got != 42 {
			t.Errorf("Get(%q) = %d (found=%v), want 42", "answer", got, ok)
		}

		missing, ok := idx.Get("nope")
		if ok || missing != 0 {
			t.Errorf("Get(%q) = %d (found=%v), want 0 and not found", "nope", missing, ok)
		}
	})
}

func TestIndexKeys(t *testing.T) {
	t.Run("empty index returns nothing", func(t *testing.T) {
		idx := New[int]()

		got := idx.Keys()
		if len(got) != 0 {
			t.Errorf("Keys() = %v, want an empty list", got)
		}
	})

	t.Run("returns every key stored", func(t *testing.T) {
		idx := New[int]()
		want := []string{"logs", "orders", "user_events"}
		for _, k := range want {
			idx.Put(k, 1)
		}

		got := idx.Keys()
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Errorf("Keys() = %v, want %v", got, want)
		}
	})

	t.Run("returns both keys of a collided bucket", func(t *testing.T) {
		if hash(collideA)%initialBuckets != hash(collideB)%initialBuckets {
			t.Fatalf("%q and %q no longer collide; pick another pair", collideA, collideB)
		}
		idx := New[int]()
		idx.Put(collideA, 1)
		idx.Put(collideB, 2)

		got := idx.Keys()
		want := []string{collideA, collideB}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("Keys() = %v, want %v — the chain was not walked to the end", got, want)
		}
	})

	t.Run("a deleted key is gone from Keys", func(t *testing.T) {
		idx := New[int]()
		idx.Put("stays", 1)
		idx.Put("goes", 2)

		idx.Delete("goes")

		got := idx.Keys()
		want := []string{"stays"}
		if !slices.Equal(got, want) {
			t.Errorf("Keys() after Delete = %v, want %v", got, want)
		}
	})
}
