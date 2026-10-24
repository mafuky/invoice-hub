// Implemented in lesson 5.

// Package callback applies supplier callbacks to provider orders.
package callback

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

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

// Callback is one supplier notification. The raw payload is already stored
// when it reaches Handle; RawInserted tells whether that store added a new
// payload (true) or found the same event_seq of the order already there (false).
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
	db    *sql.DB
	repo  *store.Repo
	locks Locker
}

func NewHandler(db *sql.DB, repo *store.Repo, locks Locker) *Handler {
	return &Handler{db: db, repo: repo, locks: locks}
}

// Handle applies one supplier callback to the provider order it names and, when
// the callback changes the order, announces the outcome with a
// provider.task.succeeded or provider.task.failed event. It returns Accepted
// for a callback that changed the order, Duplicate for one that changed
// nothing, and UnknownOrder when the supplier order does not exist here.
//
// Invariant: however often and in whatever order the supplier calls, an order
// reaches its final status once, one outcome event is written for it, and an
// order that is already final is never rewritten by an older or repeated
// callback.
//
// If a repeated callback is applied again, the invoice is moved and delivered
// three times; if a late "failed" overwrites an issued order, the issued
// invoice is marked failed and a second supplier issues it again, so the
// enterprise is billed twice.
func (h *Handler) Handle(ctx context.Context, cb Callback) (Result, error) {
	return 0, fmt.Errorf("lesson 5: %w", errors.ErrUnsupported)
}
