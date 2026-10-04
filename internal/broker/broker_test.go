package broker

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func TestBroker(t *testing.T) {
	t.Run("create then exists", func(t *testing.T) {
		b := New()
		if !b.Create("user_events") {
			t.Fatalf("Create(%q) = false, want true", "user_events")
		}
		if !b.Exists("user_events") {
			t.Errorf("Exists(%q) = false after Create, want true", "user_events")
		}
	})

	t.Run("create a duplicate topic", func(t *testing.T) {
		b := New()
		b.Create("user_events")
		if b.Create("user_events") {
			t.Errorf("second Create(%q) = true, want false", "user_events")
		}
	})

	t.Run("exists on a topic never created", func(t *testing.T) {
		b := New()
		if b.Exists("ghost") {
			t.Errorf("Exists(%q) = true, want false", "ghost")
		}
	})

	t.Run("list is sorted", func(t *testing.T) {
		b := New()
		for _, name := range []string{"orders", "logs", "user_events"} {
			b.Create(name)
		}
		got := b.List()
		want := []string{"logs", "orders", "user_events"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("List() = %v, want %v", got, want)
		}
	})

	t.Run("produce to a missing topic", func(t *testing.T) {
		b := New()
		if b.Produce("ghost", "k", []byte("v")) {
			t.Errorf("Produce(%q) = true, want false", "ghost")
		}
	})

	t.Run("produce assigns consecutive offsets", func(t *testing.T) {
		b := New()
		b.Create("user_events")
		for i := 0; i < 3; i++ {
			b.Produce("user_events", fmt.Sprintf("k%d", i), []byte("v"))
		}
		tp, _ := b.topics.Get("user_events")
		for i, m := range tp.messages {
			if m.Offset != uint32(i) {
				t.Errorf("messages[%d].Offset = %d, want %d", i, m.Offset, i)
			}
		}
	})
}

func TestBrokerConcurrency(t *testing.T) {
	t.Run("concurrent produce loses nothing", func(t *testing.T) {
		const producers = 20
		b := New()
		b.Create("user_events")

		var wg sync.WaitGroup
		for i := 0; i < producers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				b.Produce("user_events", fmt.Sprintf("k%d", i), []byte("v"))
			}(i)
		}
		wg.Wait()

		tp, _ := b.topics.Get("user_events")
		if got := len(tp.messages); got != producers {
			t.Fatalf("stored %d messages, want %d", got, producers)
		}
		seen := make(map[uint32]bool, producers)
		for _, m := range tp.messages {
			if seen[m.Offset] {
				t.Fatalf("offset %d assigned twice", m.Offset)
			}
			seen[m.Offset] = true
		}
	})

	t.Run("list while topics are created", func(t *testing.T) {
		const topics = 10
		b := New()

		var wg sync.WaitGroup
		for i := 0; i < topics; i++ {
			wg.Add(2)
			go func(i int) {
				defer wg.Done()
				b.Create(fmt.Sprintf("t%d", i))
			}(i)
			go func() {
				defer wg.Done()
				b.List()
			}()
		}
		wg.Wait()

		if got := len(b.List()); got != topics {
			t.Errorf("List() returned %d topics, want %d", got, topics)
		}
	})

	t.Run("one winner when everyone creates the same topic", func(t *testing.T) {
		const callers = 50
		b := New()

		var mu sync.Mutex
		created := 0
		var wg sync.WaitGroup
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if b.Create("user_events") {
					mu.Lock()
					created++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()

		if created != 1 {
			t.Errorf("%d callers got true from Create, want exactly 1", created)
		}
	})
}
