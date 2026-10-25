// Implemented in lesson 7.

// Package app holds the settlement service's use cases: adding delivered
// invoices to the monthly statement and moving the statement through review
// and payment.
package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/settlement/store"
	"github.com/mafuky/invoice-hub/internal/settlement/verify"
)

const consumerName = "settlement-svc"

// Verifier looks up the supplier's record of an issued invoice.
type Verifier interface {
	Lookup(ctx context.Context, line, invoiceNo string) (verify.SupplierInvoice, error)
}

// Service implements the settlement use cases.
type Service struct {
	db       *sql.DB
	repo     *store.Repo
	verifier Verifier
	now      func() time.Time
}

func NewService(db *sql.DB, repo *store.Repo, verifier Verifier) *Service {
	return &Service{db: db, repo: repo, verifier: verifier, now: time.Now}
}

// OnInvoiceDelivered is the handler of invoice.invoice.delivered events: it
// adds the delivered invoice to an open statement of its customer, starting
// with the delivery month (delivered_at in UTC as YYYY-MM), checks the invoice
// against the supplier's record and announces the item with
// settlement.item.added.
//
// Invariant: an invoice is in at most one settlement item, however often its
// delivered event arrives; the statement, the item with its verification
// result and the outbox event are written together or not at all, and a
// duplicate adds nothing and announces nothing. Only pending_settle accepts
// new items. A late invoice rolls forward one month at a time for the same
// customer until an open statement is available; a missing statement starts
// as created and becomes pending_settle.
//
// If the same delivered invoice is added twice, the customer is charged twice
// for one trip and the supplier is paid twice. Adding to a statement that has
// left pending_settle silently changes an already reviewed or paid total.
func (s *Service) OnInvoiceDelivered(ctx context.Context, env event.Envelope) error {
	return fmt.Errorf("lesson 7: %w", errors.ErrUnsupported)
}

// Calculate totals the items of a settlement into its amount and moves it
// from pending_settle to calculated.
//
// Invariant: only a settlement in pending_settle is calculated, and its
// amount is the sum of its items as they are at that moment; the amount and
// the move are written in one transaction.
//
// A stale total, or one calculated on a statement already under review, means
// the customer is billed for a different sum than the items show.
func (s *Service) Calculate(ctx context.Context, id int64) error {
	return fmt.Errorf("lesson 7: %w", errors.ErrUnsupported)
}

// SubmitReview hands a calculated settlement to the reviewer.
//
// Invariant: only a calculated settlement can be submitted.
//
// A statement submitted before it was calculated reaches the reviewer with no
// total to check.
func (s *Service) SubmitReview(ctx context.Context, id int64) error {
	return fmt.Errorf("lesson 7: %w", errors.ErrUnsupported)
}

// Approve accepts a settlement under review and releases it for payment.
//
// Invariant: only a settlement in pending_review can be approved, and
// approving it twice is refused rather than repeated.
//
// An approval that applies twice or from the wrong state lets a statement go
// to payment without a review.
func (s *Service) Approve(ctx context.Context, id int64) error {
	return fmt.Errorf("lesson 7: %w", errors.ErrUnsupported)
}

// Reject sends a settlement under review back for correction and records the
// reason. The settlement ends in pending_settle, ready to be recalculated.
//
// Invariant: only a settlement in pending_review can be rejected; it passes
// through rejected and returns to pending_settle in one call, and the reason
// is kept with the settlement. Both moves appear in the state log.
//
// If a rejected statement cannot be recalculated, finance has to delete it
// and rebuild it, and the history of what was wrong is lost.
func (s *Service) Reject(ctx context.Context, id int64, reason string) error {
	return fmt.Errorf("lesson 7: %w", errors.ErrUnsupported)
}

// Pay marks an approved settlement as paid.
//
// Invariant: only a settlement in pending_payment can be paid, and a paid
// settlement is final; paying twice is refused.
//
// Paying twice sends the same money to the supplier twice.
func (s *Service) Pay(ctx context.Context, id int64) error {
	return fmt.Errorf("lesson 7: %w", errors.ErrUnsupported)
}
