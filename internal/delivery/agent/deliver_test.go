//go:build lesson

package agent

import (
	"math/rand/v2"
	"testing"
	"time"
)

func TestNextRetry_ExponentialCappedWithJitter(t *testing.T) {
	const base, max = time.Second, 60 * time.Second
	rnd := rand.New(rand.NewPCG(7, 11))

	for _, c := range []struct {
		attempt  int
		lo, hi   time.Duration
		describe string
	}{
		{1, 500 * time.Millisecond, time.Second, "first failure"},
		{4, 4 * time.Second, 8 * time.Second, "fourth failure"},
		{10, 30 * time.Second, 60 * time.Second, "tenth failure, capped"},
	} {
		for i := 0; i < 200; i++ {
			got := NextRetry(c.attempt, base, max, rnd)
			if got < c.lo || got >= c.hi {
				t.Fatalf("%s: NextRetry(%d) = %v, want in [%v, %v)", c.describe, c.attempt, got, c.lo, c.hi)
			}
		}
	}

	a := NextRetry(5, base, max, rand.New(rand.NewPCG(3, 4)))
	b := NextRetry(5, base, max, rand.New(rand.NewPCG(3, 4)))
	if a != b {
		t.Fatalf("same seed gave %v and %v", a, b)
	}

	distinct := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		distinct[NextRetry(5, base, max, rnd)] = true
	}
	if len(distinct) < 2 {
		t.Fatal("deliveries that failed together would retry at the same instant: no jitter")
	}
}
