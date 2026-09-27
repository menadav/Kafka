package trie

import (
	"slices"
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
