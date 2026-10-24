// Implemented in lesson 1.

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
)

// TransitionInvoice moves invoice id from one state to another inside tx and
// writes one state_log row naming the cause and the event that triggered it.
// applied=false means the invoice was no longer in state from at the given
// version; nothing was changed.
//
// Invariant: the move happens only if the transition table allows it and the
// invoice is still in state from at the given version; the status change and
// its state_log row are written together or not at all.
//
// A stale event that overwrites a newer state sends an invoice backwards, so
// a delivered invoice is delivered to the customer a second time or an issued
// one is issued again by another supplier.
func (r *Repo) TransitionInvoice(ctx context.Context, tx *sql.Tx, id int64, from, to domain.InvoiceStatus, version int, cause, eventID string) (applied bool, err error) {
	return false, fmt.Errorf("lesson 1: %w", errors.ErrUnsupported)
}

// TransitionTask moves task id from one execution state to another inside tx,
// with the same guarantees as TransitionInvoice.
//
// Invariant: a task that already reached done or failed is never moved again,
// and every accepted move leaves one state_log row.
//
// A late "failed" callback that overwrites a done task makes the system
// reopen an order the supplier has already invoiced, and the enterprise pays
// tax twice for one trip.
func (r *Repo) TransitionTask(ctx context.Context, tx *sql.Tx, id int64, from, to domain.TaskStatus, version int, cause, eventID string) (applied bool, err error) {
	return false, fmt.Errorf("lesson 1: %w", errors.ErrUnsupported)
}
