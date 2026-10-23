//go:build lesson

package mq

import "testing"

func TestRetryTier_Staircase(t *testing.T) {
	cases := []struct {
		attempt int
		suffix  string
		ok      bool
	}{
		{0, "", false},
		{1, ".retry.5s", true},
		{2, ".retry.30s", true},
		{3, ".retry.5m", true},
		{4, "", false},
	}
	for _, c := range cases {
		got, ok := RetryTier(c.attempt)
		if got != c.suffix || ok != c.ok {
			t.Errorf("RetryTier(%d) = (%q, %v), want (%q, %v)", c.attempt, got, ok, c.suffix, c.ok)
		}
	}
}
