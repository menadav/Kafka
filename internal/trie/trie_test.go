package trie

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

func TestSubscribe(t *testing.T) {
	tr := New()
	tr.Subscribe("hola.amigo", "Michael")
	tr.Subscribe("amazon", "egalindo")

	got1 := tr.Match("hola.amigo")
	want1 := []string{"Michael"}
	if !slices.Equal(got1, want1) {
		t.Errorf("Match(%q) = %v, want %v", "hola.amigo", got1, want1)
	}

	got2 := tr.Match("amazon")
	want2 := []string{"egalindo"}
	if !slices.Equal(got2, want2) {
		t.Errorf("Match(%q) = %v, want %v", "amazon", got2, want2)
	}
}

func TestMatch(t *testing.T) {
	t.Run("key matching nobody", func(t *testing.T) {
		tr := New()
		tr.Subscribe("user", "client0")

		got := tr.Match("pago.ok")
		if len(got) != 0 {
			t.Errorf("Match(%q) = %v, want empty list", "pago.ok", got)
		}
	})

	t.Run("empty prefix receives everything", func(t *testing.T) {
		tr := New()
		tr.Subscribe("", "analitica")

		for _, key := range []string{"user.create", "admin.login", "anything", ""} {
			got := tr.Match(key)
			want := []string{"analitica"}
			if !slices.Equal(got, want) {
				t.Errorf("Match(%q) = %v, want %v", key, got, want)
			}
		}
	})

	t.Run("nested prefixes return several at once", func(t *testing.T) {
		tr := New()
		tr.Subscribe("", "analitica")
		tr.Subscribe("user", "client0")
		tr.Subscribe("user.create", "client7")

		got := tr.Match("user.create")
		want := []string{"analitica", "client0", "client7"}
		if !slices.Equal(got, want) {
			t.Errorf("Match(%q) = %v, want %v", "user.create", got, want)
		}
	})

	t.Run("prefix longer than the key does not match", func(t *testing.T) {
		tr := New()
		tr.Subscribe("user.create", "client7")

		got := tr.Match("user")
		if len(got) != 0 {
			t.Errorf("Match(%q) = %v, want empty list", "user", got)
		}
	})
}

func TestUnsubscribe(t *testing.T) {
	t.Run("removing one subscriber leaves the others", func(t *testing.T) {
		tr := New()
		tr.Subscribe("user", "client0")
		tr.Subscribe("user", "client7")

		tr.Unsubscribe("user", "client0")

		got := tr.Match("user")
		want := []string{"client7"}
		if !slices.Equal(got, want) {
			t.Errorf("after Unsubscribe(%q, %q), Match(%q) = %v, want %v",
				"user", "client0", "user", got, want)
		}
	})

	t.Run("removing a subscriber that is not there changes nothing", func(t *testing.T) {
		tr := New()
		tr.Subscribe("user", "client0")

		tr.Unsubscribe("user", "ghost")

		got := tr.Match("user")
		want := []string{"client0"}
		if !slices.Equal(got, want) {
			t.Errorf("after Unsubscribe(%q, %q), Match(%q) = %v, want %v",
				"user", "ghost", "user", got, want)
		}
	})
}

func TestPrefixRules(t *testing.T) {
	cases := []struct {
		name   string
		prefix string
		key    string
		match  bool
	}{
		{"exact match", "user", "user", true},
		{"prefix match", "user", "user.login", true},
		{"no match", "user", "admin", false},
		{"wildcard", "", "admin", true},
		{"wildcard on an empty key", "", "", true},
		{"shares letters but is not a prefix", "user", "users", true},
		{"key shorter than the prefix", "user.login", "user", false},
		{"differs in the middle", "user.create", "user.delete", false},
		{"case matters", "User", "user", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tr := New()
			tr.Subscribe(c.prefix, "client0")
			got := slices.Contains(tr.Match(c.key), "client0")
			if got != c.match {
				t.Errorf("prefix %q, key %q: matched=%v, want %v", c.prefix, c.key, got, c.match)
			}
		})
	}
}

func TestEdgeCases(t *testing.T) {
	t.Run("empty trie matches nothing", func(t *testing.T) {
		tr := New()
		for _, key := range []string{"", "a", "user.login"} {
			if got := tr.Match(key); len(got) != 0 {
				t.Errorf("Match(%q) on an empty trie = %v, want nothing", key, got)
			}
		}
	})

	t.Run("one subscriber", func(t *testing.T) {
		tr := New()
		tr.Subscribe("a", "only")
		if got := tr.Match("a"); !slices.Equal(got, []string{"only"}) {
			t.Errorf("Match(%q) = %v, want [only]", "a", got)
		}
		if got := tr.Match("b"); len(got) != 0 {
			t.Errorf("Match(%q) = %v, want nothing", "b", got)
		}
	})

	t.Run("several consumers share a prefix", func(t *testing.T) {
		tr := New()
		for _, id := range []string{"c1", "c2", "c3"} {
			tr.Subscribe("user", id)
		}
		got := tr.Match("user.login")
		slices.Sort(got)
		if want := []string{"c1", "c2", "c3"}; !slices.Equal(got, want) {
			t.Errorf("Match = %v, want %v", got, want)
		}
	})

	t.Run("unsubscribing the last one leaves the trie empty for that key", func(t *testing.T) {
		tr := New()
		tr.Subscribe("user", "c1")
		tr.Unsubscribe("user", "c1")
		if got := tr.Match("user.login"); len(got) != 0 {
			t.Errorf("Match after unsubscribing everyone = %v, want nothing", got)
		}
	})

	t.Run("unsubscribing from an empty trie does not panic", func(t *testing.T) {
		tr := New()
		tr.Unsubscribe("user", "ghost")
		tr.Unsubscribe("", "ghost")
	})
}

func TestLargeScale(t *testing.T) {
	const subscribers = 5000
	const keys = 2000
	rng := rand.New(rand.NewPCG(1, 2))
	word := func(max int) string {
		b := make([]byte, rng.IntN(max+1))
		for i := range b {
			b[i] = "abc."[rng.IntN(4)]
		}
		return string(b)
	}

	tr := New()
	prefixes := make(map[string]string, subscribers)
	for i := 0; i < subscribers; i++ {
		id := fmt.Sprintf("c%d", i)
		prefixes[id] = word(6)
		tr.Subscribe(prefixes[id], id)
	}

	for i := 0; i < keys; i++ {
		key := word(10)
		var want []string
		for id, prefix := range prefixes {
			if strings.HasPrefix(key, prefix) {
				want = append(want, id)
			}
		}
		got := tr.Match(key)
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("Match(%q): trie found %d consumers, a brute-force scan finds %d", key, len(got), len(want))
		}
	}
}
