// Implemented in lesson 9.

package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// RedFlush starts the red flush of a closed invoice: it records a RED_FLUSH
// command and a task that asks the supplier to cancel the invoice with an
// offsetting negative one. It returns the command id.
//
// Invariant: only a closed invoice is accepted; other states are refused with
// no writes. An invoice has at most one active red command (RED_FLUSH or REISSUE);
// another request while one is active is refused with no writes.
// Settlement adjustment for red-flushed invoices is not modeled.
//
// A red flush of an invoice that was never issued, or one that runs twice,
// leaves finance with a negative entry that has no invoice behind it.
// Contract: persistence must use the injected handle/insert fields and repo, never consume.Handle/outbox.Insert directly; the configured store implementation selects them.
func (s *Service) RedFlush(ctx context.Context, invoiceID int64, reason string) (commandID int64, err error) {
	return 0, fmt.Errorf("lesson 9: %w", errors.ErrUnsupported)
}

// Reissue red flushes a closed invoice and requests a replacement using its
// existing product line, title, tax number, amount and customer; the API carries
// no new details. It returns the command id. Other invoice states or an active
// RED_FLUSH/REISSUE command are refused with no writes.
//
// Invariant: the new invoice is requested only after the old one has been
// red flushed, and exactly one new invoice results from one reissue command.
//
// If both invoices are live at once the enterprise has claimed the same trip
// twice; if the old one is red flushed and the new one never issued, the trip
// has no invoice at all.
// Contract: persistence must use the injected handle/insert fields and repo, never consume.Handle/outbox.Insert directly; the configured store implementation selects them.
func (s *Service) Reissue(ctx context.Context, invoiceID int64, reason string) (commandID int64, err error) {
	return 0, fmt.Errorf("lesson 9: %w", errors.ErrUnsupported)
}

// Supplement issues an invoice that was missed or failed earlier, as a new
// invoice with a SUPPLEMENT command. It returns the new invoice and command ids.
// The input needs a biz_order_no that is not in use yet (for example the
// original plus "-S1"); one that already has an invoice is refused with an
// error wrapping ErrBizOrderExists, which the gRPC layer reports as AlreadyExists.
//
// Invariant: a supplement for a biz_order_no that already has a live invoice
// is refused; it never creates a second live one.
//
// A supplement on top of an invoice that was in fact issued puts a duplicate
// invoice on the books and the customer is invoiced twice for one trip.
// Contract: persistence must use the injected handle/insert fields and repo, never consume.Handle/outbox.Insert directly; the configured store implementation selects them.
func (s *Service) Supplement(ctx context.Context, in ApplyInput) (invoiceID, commandID int64, err error) {
	return 0, 0, fmt.Errorf("lesson 9: %w", errors.ErrUnsupported)
}

// ErrBizOrderExists is returned when a command needs a biz_order_no that already has an invoice.
var ErrBizOrderExists = errors.New("biz_order_no already has an invoice")

// Reopen replaces a failed or timed-out task with a new attempt for the same
// command, so the invoice keeps progressing. The new task has no supplier,
// leaving the choice to provider-svc, and it returns the new task's id.
//
// Invariant: a command has at most one live task at a time, and the new task
// together with its invoice.task.created outbox event is written in the
// caller's transaction tx.
// Replaying the failed attempt returns the existing live task's id without writes.
//
// If the replacement is never created the invoice sits in issuing forever; if
// two live tasks exist, two suppliers are asked at once and the same trip is
// invoiced twice.
// Contract: persistence must use the injected handle/insert fields and repo, never consume.Handle/outbox.Insert directly; the configured store implementation selects them.
func (s *Service) Reopen(ctx context.Context, tx *sql.Tx, invoiceID, commandID, failedTaskID int64) (newTaskID int64, err error) {
	return 0, fmt.Errorf("lesson 9: %w", errors.ErrUnsupported)
}

// IssueReplacement creates the new invoice of a reissue once the old invoice
// has been red flushed: an invoice for the same trip under a biz_order_no made
// from the old one plus "-R<n>", with its own ISSUE command, pending task and
// invoice.task.created outbox event, all in the caller's transaction tx. It
// returns the new invoice's id, including on replay of the same command.
// The replacement copies the old invoice's product line, title, tax number,
// amount and customer, since the reissue request carries no new details.
//
// Invariant: one reissue command yields exactly one replacement invoice, and
// the old invoice ends red_flushed.
//
// If no replacement appears the employee is left without a valid invoice for
// a changed trip; if two appear, the enterprise is invoiced twice.
// Contract: persistence must use the injected handle/insert fields and repo, never consume.Handle/outbox.Insert directly; the configured store implementation selects them.
func (s *Service) IssueReplacement(ctx context.Context, tx *sql.Tx, oldInvoiceID, reissueCommandID int64) (newInvoiceID int64, err error) {
	return 0, fmt.Errorf("lesson 9: %w", errors.ErrUnsupported)
}
