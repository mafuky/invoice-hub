// Implemented in lesson 9.

package app

import (
	"context"
	"errors"
	"fmt"
)

// RedFlush starts the red flush of an issued invoice: it records a RED_FLUSH
// command and a task that asks the supplier to cancel the invoice with an
// offsetting negative one. It returns the command id.
//
// Invariant: only an issued invoice can be red flushed, and an invoice gets at
// most one red flush that is not yet finished.
//
// A red flush of an invoice that was never issued, or one that runs twice,
// leaves finance with a negative entry that has no invoice behind it.
func (s *Service) RedFlush(ctx context.Context, invoiceID int64, reason string) (commandID int64, err error) {
	return 0, fmt.Errorf("lesson 9: %w", errors.ErrUnsupported)
}

// Reissue handles a re-booked trip: the old invoice is red flushed and a new
// invoice for the changed amount or details is issued. It returns the command id.
//
// Invariant: the new invoice is requested only after the old one has been
// red flushed, and exactly one new invoice results from one reissue command.
//
// If both invoices are live at once the enterprise has claimed the same trip
// twice; if the old one is red flushed and the new one never issued, the trip
// has no invoice at all.
func (s *Service) Reissue(ctx context.Context, invoiceID int64, reason string) (commandID int64, err error) {
	return 0, fmt.Errorf("lesson 9: %w", errors.ErrUnsupported)
}

// Supplement issues an invoice that was missed or failed earlier, as a new
// invoice with a SUPPLEMENT command. It returns the new invoice and command ids.
//
// Invariant: a supplement for a biz_order_no that already has a live invoice
// is refused or returns that invoice; it never creates a second live one.
//
// A supplement on top of an invoice that was in fact issued puts a duplicate
// invoice on the books and the customer is invoiced twice for one trip.
func (s *Service) Supplement(ctx context.Context, in ApplyInput) (invoiceID, commandID int64, err error) {
	return 0, 0, fmt.Errorf("lesson 9: %w", errors.ErrUnsupported)
}
