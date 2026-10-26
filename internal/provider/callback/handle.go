// Implemented in lesson 5.

// Package callback applies supplier callbacks to provider orders.
package callback

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mafuky/invoice-hub/internal/platform/outbox"
	"github.com/mafuky/invoice-hub/internal/provider/store"
)

// Outcome is what the supplier reports for an order.
type Outcome string

const (
	OutcomeSucceeded Outcome = "SUCCEEDED"
	OutcomeFailed    Outcome = "FAILED"
)

// Result is how a callback was received.
type Result int

const (
	Accepted Result = iota + 1
	Duplicate
	UnknownOrder
)

func (r Result) String() string {
	switch r {
	case Accepted:
		return "accepted"
	case Duplicate:
		return "duplicate"
	case UnknownOrder:
		return "unknown_order"
	}
	return fmt.Sprintf("result(%d)", int(r))
}

// ErrBusy means another callback for the same order is being processed. The
// callback was not applied and the supplier should send it again; it is never
// a duplicate.
var ErrBusy = errors.New("supplier order is being processed")

// Callback is one supplier notification. The raw payload is already stored
// when it reaches Handle. RawInserted tells whether that store added a new
// payload (true) or found the same event_seq of the order already there
// (false); it is for logs only, and whether the callback takes effect is
// decided by the order in MySQL, never by this flag.
type Callback struct {
	Supplier        string
	SupplierOrderNo string
	EventSeq        int64
	Outcome         Outcome
	InvoiceNo       string
	RawJSON         string
	RawInserted     bool
}

// Locker is a mutual-exclusion lock with an expiry.
type Locker interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (token string, ok bool, err error)
	Release(ctx context.Context, key, token string) error
}

// Handler applies callbacks to provider orders.
type Handler struct {
	insert outbox.Inserter
	db     *sql.DB
	repo   store.Store
	locks  Locker
}

func NewHandler(db *sql.DB, repo store.Store, locks Locker, options ...Option) *Handler {
	s := &Handler{db: db, repo: repo, locks: locks, insert: outbox.Insert}
	for _, option := range options {
		option(s)
	}
	return s
}

// Handle applies one supplier callback to the provider order it names and, when
// the callback changes the order, writes a provider.task.succeeded or
// provider.task.failed event in the same transaction. It returns Accepted for
// a callback that changed the order, Duplicate for one that changed nothing,
// UnknownOrder when the supplier order does not exist here, and ErrBusy when
// another callback for the order is in flight.
//
// Invariant: however often and in whatever order the supplier calls, each
// callback takes effect at most once and one outcome event is written per
// change. An order that has succeeded stays succeeded: a failed callback never
// rewrites it, and a callback with an older or equal event_seq never changes
// the order. A succeeded callback with a newer event_seq than a recorded
// failure is applied. A busy order is reported as ErrBusy, never as Duplicate.
// The MongoDB unique index on raw payloads cannot be the guard, because it does
// not commit together with the MySQL order and event.
//
// If a repeated callback is applied again, the invoice is moved and delivered
// three times; if a late "failed" overwrites an issued order, the issued
// invoice is marked failed and a second supplier issues it again, so the
// enterprise is billed twice; if a busy order is answered as a duplicate, the
// supplier stops retrying and the invoice stays in issuing.
func (h *Handler) Handle(ctx context.Context, cb Callback) (Result, error) {
	return 0, fmt.Errorf("lesson 5: %w", errors.ErrUnsupported)
}

// Option configures the service's persistence helpers before it starts.
type Option func(*Handler)

// WithPersistence binds event writes to the selected store implementation.
func WithPersistence(insert outbox.Inserter) Option {
	return func(s *Handler) { s.insert = insert }
}
