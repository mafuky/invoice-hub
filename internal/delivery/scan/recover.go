// Implemented in lesson 6.

// Package scan takes back deliveries that were abandoned halfway.
package scan

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/mafuky/invoice-hub/internal/delivery/store"
)

// Scanner finds deliveries stuck in delivering.
type Scanner struct {
	repo *store.Repo
	now  func() time.Time
}

// NewScanner scans the deliveries kept in repo.
func NewScanner(repo *store.Repo) *Scanner { return &Scanner{repo: repo, now: time.Now} }

// RecoverStuck puts back to pending every delivery that has been delivering
// for longer than olderThan, and returns how many it put back.
//
// Invariant: a delivery whose worker died is picked up again, and a delivery
// that is still being pushed, or has just finished, is never taken away from
// its worker.
//
// If a delivery stays in delivering after its process was killed mid-push,
// that invoice is never delivered and no one notices; if a live push is taken
// away, the customer receives the invoice twice.
func (s *Scanner) RecoverStuck(ctx context.Context, olderThan time.Duration) (recovered int, err error) {
	return 0, fmt.Errorf("lesson 6: %w", errors.ErrUnsupported)
}

// Run calls RecoverStuck every interval until ctx is cancelled; a failed pass is logged and retried.
func (s *Scanner) Run(ctx context.Context, interval, olderThan time.Duration) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		n, err := s.RecoverStuck(ctx, olderThan)
		if err != nil {
			if ctx.Err() == nil {
				slog.WarnContext(ctx, "recover stuck deliveries failed", "err", err)
			}
			continue
		}
		if n > 0 {
			slog.InfoContext(ctx, "recovered stuck deliveries", "count", n)
		}
	}
}
