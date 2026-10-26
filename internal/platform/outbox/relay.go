// Implemented in lesson 2.

package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/mafuky/invoice-hub/internal/platform/event"
)

const (
	pollInterval = 500 * time.Millisecond
	maxErrWait   = 10 * time.Second
	maxBackoff   = time.Minute
)

// Publisher sends one event and returns only after the broker has confirmed it.
type Publisher interface {
	Publish(ctx context.Context, routingKey string, env event.Envelope) error
}

// Relay moves outbox rows of one service database to the broker.
type Relay struct {
	db   *sql.DB
	pub  Publisher
	name string
	log  *slog.Logger
}

// NewRelay builds a relay for the outbox table reachable through db; name tags its logs.
func NewRelay(db *sql.DB, pub Publisher, name string) *Relay {
	return &Relay{db: db, pub: pub, name: name, log: slog.Default().With("relay", name)}
}

// Backoff is the wait before a failed outbox row is tried again.
func Backoff(attempts int) time.Duration {
	if attempts < 0 {
		attempts = 0
	}
	if attempts > 6 {
		return maxBackoff
	}
	return min(time.Second<<attempts, maxBackoff)
}

// RunOnce sends one batch of due outbox rows and marks them sent.
//
// Invariant: a row is marked sent only after the broker confirmed it, and a
// row that failed to send is retried later and never skipped; two relays
// working on the same table must not send the same event. A publisher-closed
// error (one wrapping amqp.ErrClosed) aborts the pass and is returned, and the
// row is not marked failed; Run treats it as fatal so the process restarts.
//
// If an invoice's "task created" event is lost, the invoice stays in
// applying forever and no supplier is ever asked to issue it; if it is sent
// twice, the supplier gets two orders for one biz_order_no.
func (r *Relay) RunOnce(ctx context.Context) (sent int, err error) {
	return 0, fmt.Errorf("lesson 2: %w", errors.ErrUnsupported)
}

// Run polls the outbox until ctx is cancelled. It returns an error when the
// publisher's channel is closed, because the process must then restart.
func (r *Relay) Run(ctx context.Context) error {
	return r.run(ctx, r.RunOnce)
}

func (r *Relay) run(ctx context.Context, runOnce func(context.Context) (int, error)) error {
	wait := time.Duration(0)
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		n, err := runOnce(ctx)
		switch {
		case err == nil:
			failures = 0
			wait = 0
			if n == 0 {
				wait = pollInterval
			}
		case errors.Is(err, amqp.ErrClosed):
			return fmt.Errorf("outbox %s: publisher closed: %w", r.name, err)
		case ctx.Err() != nil:
			return nil
		default:
			failures++
			wait = min(pollInterval<<min(failures, 5), maxErrWait)
			r.log.WarnContext(ctx, "outbox relay pass failed", "sent", n, "err", err)
		}
	}
}
