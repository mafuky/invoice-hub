// Implemented in lesson 1.

package domain

// CanTo reports whether an invoice may move from s to next according to InvoiceTransitions.
//
// Invariant: only moves listed in the table are allowed; staying in the same
// state, moving backwards and leaving a final state are all refused.
//
// If an out-of-order event could jump an invoice from issuing straight to
// delivered, the customer would get an invoice the supplier never confirmed
// and settlement would pay for it.
func (s InvoiceStatus) CanTo(next InvoiceStatus) bool { return false }

// CanTo reports whether a supplier task may move from s to next according to TaskTransitions.
//
// Invariant: a task that is done or failed never runs again; a late callback
// must not reopen it.
//
// A reopened task would send the same biz_order_no to a supplier twice, and
// the enterprise would pay tax on two invoices.
func (s TaskStatus) CanTo(next TaskStatus) bool { return false }

// CanTo reports whether a command may move from s to next according to CommandTransitions.
//
// Invariant: a command that has succeeded or failed is final.
//
// A red flush command that flips from failed back to dispatched could cancel
// an invoice twice and leave finance with two negative entries.
func (s CommandStatus) CanTo(next CommandStatus) bool { return false }
