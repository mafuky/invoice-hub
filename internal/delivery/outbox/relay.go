package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/mafuky/invoice-hub/internal/platform/event"
	platform "github.com/mafuky/invoice-hub/internal/platform/outbox"
)

const (
	pollInterval = 500 * time.Millisecond
	maxErrWait   = 10 * time.Second
	batchSize    = 50
	// leaseFor outlasts the publisher's confirm timeout, so a row being sent is not claimed twice.
	leaseFor = 30 * time.Second
)

// Relay moves delivery-svc outbox documents to the broker.
type Relay struct {
	rows rows
	pub  platform.Publisher
	name string
	log  *slog.Logger
	now  func() time.Time
}

// NewRelay builds a relay for the outbox collection coll; name tags its logs.
func NewRelay(coll *mongo.Collection, pub platform.Publisher, name string) *Relay {
	return newRelay(mongoRows{coll: coll}, pub, name)
}

func newRelay(r rows, pub platform.Publisher, name string) *Relay {
	return &Relay{rows: r, pub: pub, name: name, log: slog.Default().With("relay", name), now: time.Now}
}

// RunOnce sends one batch of due documents and marks each sent after the broker
// confirmed it. A failed send is backed off and the pass stops with that error;
// a publisher-closed error is returned as is and the row is not counted as a
// failed attempt.
func (r *Relay) RunOnce(ctx context.Context) (sent int, err error) {
	for sent < batchSize {
		row, ok, err := r.rows.claim(ctx, r.now().UTC(), leaseFor)
		if err != nil {
			return sent, err
		}
		if !ok {
			return sent, nil
		}

		var env event.Envelope
		if err := json.Unmarshal([]byte(row.Body), &env); err != nil {
			if merr := r.fail(ctx, row, fmt.Errorf("decode body: %w", err)); merr != nil {
				return sent, merr
			}
			continue
		}
		if perr := r.pub.Publish(ctx, row.RoutingKey, env); perr != nil {
			if errors.Is(perr, amqp.ErrClosed) {
				_ = r.rows.release(context.WithoutCancel(ctx), row.ID, r.now().UTC())
				return sent, fmt.Errorf("outbox %s: publish %s: %w", r.name, row.RoutingKey, perr)
			}
			if merr := r.fail(ctx, row, perr); merr != nil {
				return sent, merr
			}
			return sent, fmt.Errorf("outbox %s: publish %s: %w", r.name, row.RoutingKey, perr)
		}
		// The event is already out; record it even if shutdown has started.
		if err := r.rows.markSent(context.WithoutCancel(ctx), row.ID, r.now().UTC()); err != nil {
			return sent, err
		}
		sent++
	}
	return sent, nil
}

func (r *Relay) fail(ctx context.Context, row row, cause error) error {
	attempts := row.Attempts + 1
	next := r.now().UTC().Add(platform.Backoff(attempts))
	return r.rows.markFailed(context.WithoutCancel(ctx), row.ID, attempts, next, cause.Error())
}

// Run polls the outbox until ctx is cancelled. It returns an error when the
// publisher's channel is closed, because the process must then restart.
func (r *Relay) Run(ctx context.Context) error {
	wait := time.Duration(0)
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		n, err := r.RunOnce(ctx)
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
