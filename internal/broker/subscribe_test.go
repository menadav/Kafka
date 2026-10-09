package broker

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeChannel struct {
	mu      sync.Mutex
	got     []Message
	broken  bool
	goodbye bool
	closed  bool
}

func (f *fakeChannel) Send(m Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.broken {
		return errors.New("broken channel")
	}
	f.got = append(f.got, m)
	return nil
}

func (f *fakeChannel) Goodbye() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.goodbye = true
	return nil
}

func (f *fakeChannel) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeChannel) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, len(f.got))
	for i, m := range f.got {
		keys[i] = m.Key
	}
	return keys
}

func (f *fakeChannel) state() (goodbye, closed bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.goodbye, f.closed
}

func (f *fakeChannel) waitFor(t *testing.T, n int) []string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for len(f.keys()) < n && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond)
	return f.keys()
}

func attach(t *testing.T, b *Broker, topic, id, prefix string) *fakeChannel {
	t.Helper()
	if err := b.Subscribe(topic, id, prefix, "/tmp/test."+id, 0, false); err != nil {
		t.Fatalf("Subscribe(%q, %q) = %v", topic, id, err)
	}
	ch := &fakeChannel{}
	if err := b.Attach(id, ch); err != nil {
		t.Fatalf("Attach(%q) = %v", id, err)
	}
	return ch
}

func produceAll(b *Broker, topic string, keys ...string) {
	for _, k := range keys {
		b.Produce(topic, k, []byte("v"))
	}
}

func matching(keys []string, prefix string) []string {
	var out []string
	for _, k := range keys {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out
}

func TestFilteringIntegration(t *testing.T) {
	b := New()
	b.Create("user_events")
	prefixes := map[string]string{
		"everything": "",
		"users":      "user",
		"creates1":   "user.create",
		"creates2":   "user.create",
		"admins":     "admin",
		"nobody":     "zzz",
	}
	channels := map[string]*fakeChannel{}
	for id, prefix := range prefixes {
		channels[id] = attach(t, b, "user_events", id, prefix)
	}

	keys := []string{"user", "user.login", "user.create", "user.create.42", "admin", "administrator", "other", ""}
	produceAll(b, "user_events", keys...)

	for id, prefix := range prefixes {
		want := matching(keys, prefix)
		got := channels[id].waitFor(t, len(want))
		if !slices.Equal(got, want) {
			t.Errorf("%s (prefix %q) got %q, want %q", id, prefix, got, want)
		}
	}
}

func TestSubscribeCases(t *testing.T) {
	t.Run("unknown topic", func(t *testing.T) {
		b := New()
		err := b.Subscribe("ghost", "c0", "", "/x", 0, false)
		if !errors.Is(err, ErrTopicNotFound) {
			t.Errorf("Subscribe on a missing topic = %v, want ErrTopicNotFound", err)
		}
	})

	t.Run("a name that is connected cannot subscribe again", func(t *testing.T) {
		b := New()
		b.Create("t")
		b.Subscribe("t", "c0", "", "/x", 0, false)
		err := b.Subscribe("t", "c0", "", "/x", 0, false)
		if !errors.Is(err, ErrClientBusy) {
			t.Errorf("second Subscribe = %v, want ErrClientBusy", err)
		}
	})

	t.Run("a returning client keeps its committed offset", func(t *testing.T) {
		b := New()
		b.Create("t")
		b.Subscribe("t", "c0", "", "/x", 0, false)
		b.Commit("c0", 4)
		b.Disconnect("c0")
		if err := b.Subscribe("t", "c0", "", "/x", 0, false); err != nil {
			t.Fatalf("returning Subscribe = %v", err)
		}
		if c, _ := b.Info("c0"); c.Offset != 4 {
			t.Errorf("offset after returning = %d, want 4", c.Offset)
		}
	})

	t.Run("an explicit offset wins over the committed one", func(t *testing.T) {
		b := New()
		b.Create("t")
		b.Subscribe("t", "c0", "", "/x", 0, false)
		b.Commit("c0", 4)
		b.Disconnect("c0")
		b.Subscribe("t", "c0", "", "/x", 1, true)
		if c, _ := b.Info("c0"); c.Offset != 1 {
			t.Errorf("offset with --offset 1 = %d, want 1", c.Offset)
		}
	})

	t.Run("moving to another topic stops the old deliveries", func(t *testing.T) {
		b := New()
		b.Create("a")
		b.Create("b")
		old := attach(t, b, "a", "c0", "")
		b.Disconnect("c0")
		next := attach(t, b, "b", "c0", "")
		produceAll(b, "a", "for-a")
		produceAll(b, "b", "for-b")
		if got := next.waitFor(t, 1); !slices.Equal(got, []string{"for-b"}) {
			t.Errorf("new topic got %q, want [for-b]", got)
		}
		if got := old.keys(); len(got) != 0 {
			t.Errorf("old topic still delivered %q", got)
		}
	})

	t.Run("many subscribe at once with the same name: one wins", func(t *testing.T) {
		b := New()
		b.Create("t")
		var wg sync.WaitGroup
		var mu sync.Mutex
		wins := 0
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if b.Subscribe("t", "same", "p", "/x", 0, false) == nil {
					mu.Lock()
					wins++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if wins != 1 {
			t.Errorf("%d subscribes succeeded, want exactly 1", wins)
		}
	})
}

func TestDelivery(t *testing.T) {
	t.Run("a late subscriber first gets the history, then new messages", func(t *testing.T) {
		b := New()
		b.Create("t")
		produceAll(b, "t", "a1", "b1", "a2")
		ch := attach(t, b, "t", "late", "a")
		produceAll(b, "t", "a3", "b2")
		if got, want := ch.waitFor(t, 3), []string{"a1", "a2", "a3"}; !slices.Equal(got, want) {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("an explicit offset skips older messages", func(t *testing.T) {
		b := New()
		b.Create("t")
		produceAll(b, "t", "m0", "m1", "m2", "m3")
		b.Subscribe("t", "c0", "", "/x", 2, true)
		ch := &fakeChannel{}
		b.Attach("c0", ch)
		if got, want := ch.waitFor(t, 2), []string{"m2", "m3"}; !slices.Equal(got, want) {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("each subscriber gets its messages in offset order", func(t *testing.T) {
		const n = 300
		b := New()
		b.Create("t")
		chans := []*fakeChannel{attach(t, b, "t", "s1", ""), attach(t, b, "t", "s2", ""), attach(t, b, "t", "s3", "")}
		for i := 0; i < n; i++ {
			b.Produce("t", fmt.Sprintf("k%d", i), nil)
		}
		for i, ch := range chans {
			ch.waitFor(t, n)
			ch.mu.Lock()
			for j, m := range ch.got {
				if m.Offset != uint32(j) {
					t.Errorf("subscriber %d: message %d has offset %d", i, j, m.Offset)
					break
				}
			}
			ch.mu.Unlock()
		}
	})

	t.Run("a broken channel disconnects its client", func(t *testing.T) {
		b := New()
		b.Create("t")
		ch := attach(t, b, "t", "c0", "")
		ch.mu.Lock()
		ch.broken = true
		ch.mu.Unlock()
		produceAll(b, "t", "k")
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if c, _ := b.Info("c0"); !c.Connected {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Error("client still connected after its channel failed")
	})

	t.Run("a failure from an old session does not disconnect the new one", func(t *testing.T) {
		b := New()
		b.Create("t")
		attach(t, b, "t", "c0", "")
		old, _ := b.clients.Get("c0")
		oldSession := old.sub
		b.Disconnect("c0")
		attach(t, b, "t", "c0", "")
		b.dropSubscriber(oldSession)
		if c, _ := b.Info("c0"); !c.Connected {
			t.Error("the new session was disconnected by the old one's failure")
		}
	})

	t.Run("after Disconnect nothing more is sent", func(t *testing.T) {
		b := New()
		b.Create("t")
		ch := attach(t, b, "t", "c0", "")
		produceAll(b, "t", "before")
		ch.waitFor(t, 1)
		b.Disconnect("c0")
		produceAll(b, "t", "after")
		if got := ch.waitFor(t, 2); !slices.Equal(got, []string{"before"}) {
			t.Errorf("got %q, want only [before]", got)
		}
		if _, closed := ch.state(); !closed {
			t.Error("channel not closed after Disconnect")
		}
	})
}

func TestCommitAndInfo(t *testing.T) {
	b := New()
	b.Create("user_events")
	b.Subscribe("user_events", "client0", "user.create", "/tmp/s.client0", 0, false)

	if err := b.Commit("client0", 4); err != nil {
		t.Fatalf("Commit = %v", err)
	}
	c, ok := b.Info("client0")
	if !ok {
		t.Fatal("Info(client0) not found")
	}
	if c.ClientID != "client0" || c.Topic != "user_events" || c.Offset != 4 ||
		c.Prefix != "user.create" || c.IPCPath != "/tmp/s.client0" || !c.Connected {
		t.Errorf("Info = %+v, want client0 on user_events, offset 4, prefix user.create, connected", c)
	}

	if _, ok := b.Info("ghost"); ok {
		t.Error("Info(ghost) found a client")
	}
	if err := b.Commit("ghost", 1); !errors.Is(err, ErrClientNotFound) {
		t.Errorf("Commit(ghost) = %v, want ErrClientNotFound", err)
	}

	b.Disconnect("client0")
	if err := b.Commit("client0", 9); !errors.Is(err, ErrClientNotFound) {
		t.Errorf("Commit after Disconnect = %v, want ErrClientNotFound", err)
	}
	if c, ok := b.Info("client0"); !ok || c.Offset != 4 || c.Connected {
		t.Errorf("Info after Disconnect = %+v, %v; want offset 4, not connected", c, ok)
	}
}

func TestShutdown(t *testing.T) {
	t.Run("says goodbye once everything is committed", func(t *testing.T) {
		b := New()
		b.Create("t")
		ch := attach(t, b, "t", "c0", "")
		produceAll(b, "t", "k")
		ch.waitFor(t, 1)
		b.Commit("c0", 1)

		start := time.Now()
		b.Shutdown(5 * time.Second)
		if took := time.Since(start); took > time.Second {
			t.Errorf("Shutdown took %v with nothing pending", took)
		}
		if goodbye, closed := ch.state(); !goodbye || !closed {
			t.Errorf("goodbye=%v closed=%v, want both true", goodbye, closed)
		}
	})

	t.Run("gives up waiting for commits after the timeout", func(t *testing.T) {
		b := New()
		b.Create("t")
		ch := attach(t, b, "t", "c0", "")
		produceAll(b, "t", "never-acked")
		ch.waitFor(t, 1)

		start := time.Now()
		b.Shutdown(200 * time.Millisecond)
		took := time.Since(start)
		if took < 200*time.Millisecond || took > time.Second {
			t.Errorf("Shutdown took %v, want about 200ms", took)
		}
		if goodbye, _ := ch.state(); !goodbye {
			t.Error("no goodbye after the timeout")
		}
	})
}
