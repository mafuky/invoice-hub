// Implemented in lesson 9.

package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	providerv1 "github.com/mafuky/invoice-hub/gen/invoicehub/provider/v1"
	"github.com/mafuky/invoice-hub/internal/invoice/store"
)

// Scanner recovers tasks whose supplier never called back.
type Scanner struct {
	db       *sql.DB
	repo     *store.Repo
	provider providerv1.ProviderServiceClient
	now      func() time.Time
}

func NewScanner(db *sql.DB, repo *store.Repo, provider providerv1.ProviderServiceClient) *Scanner {
	return &Scanner{db: db, repo: repo, provider: provider, now: time.Now}
}

// RunOnce handles the tasks that are waiting_callback past their deadline. For
// each it asks the supplier, through provider-svc QueryOrder, whether the
// invoice was issued. A found result advances the task as the callback would
// have. If the supplier has no such order, the task is marked failed and a new
// pending task with no supplier is created for the same command, with a
// "task created" outbox event, so provider-svc picks the next supplier.
//
// Invariant: an order the supplier has already issued is never reordered, and a
// timed-out order the supplier does not know is retried exactly once per
// timeout; the failed task, the new task and the event commit together.
//
// If a lost callback is treated as "not issued", a second supplier issues the
// same trip and the enterprise pays tax twice; if the task is never retried,
// the invoice stays in issuing forever.
func (s *Scanner) RunOnce(ctx context.Context) (handled int, err error) {
	return 0, fmt.Errorf("lesson 9: %w", errors.ErrUnsupported)
}

// Run scans every interval until ctx is cancelled.
func (s *Scanner) Run(ctx context.Context, interval time.Duration) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		n, err := s.RunOnce(ctx)
		if err != nil {
			slog.WarnContext(ctx, "timeout scan failed", "handled", n, "err", err)
			continue
		}
		if n > 0 {
			slog.InfoContext(ctx, "timed-out tasks handled", "count", n)
		}
	}
}
