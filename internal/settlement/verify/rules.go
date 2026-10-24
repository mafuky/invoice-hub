// Implemented in lesson 8.

// Package verify compares our invoice records with what the supplier reports.
package verify

import "github.com/mafuky/invoice-hub/internal/platform/money"

// Check names, in the order Run reports them.
const (
	CheckExists = "exists"
	CheckAmount = "amount"
	CheckTitle  = "title"
	CheckStatus = "status"
)

// Check is the outcome of one comparison.
type Check struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail,omitempty"`
}

// InvoiceView is our side of the comparison. Status is the state the supplier
// is expected to report for it, "issued" for a live invoice.
type InvoiceView struct {
	Amount money.Amount
	Title  string
	Status string
}

// SupplierInvoice is the supplier-side record. Found=false means the supplier
// has no such invoice; the other fields are then empty.
type SupplierInvoice struct {
	Found  bool
	Amount money.Amount
	Title  string
	Status string
}

// Run checks a delivered invoice against the supplier's record and returns
// one Check for each of exists, amount, title and status.
//
// Invariant: every mismatch shows up as a failed check with a detail saying
// what differs. If the supplier has no such invoice, exists fails and the
// remaining checks are reported as not passed. Product line rules apply on top
// of the common checks: FLIGHT also requires the supplier to report the
// invoice as issued, HOTEL accepts an amount that differs by at most 0.01, and
// TRAIN uses the common checks only.
//
// An amount that is off by one cent on a flight, a wrong buyer title, or an
// invoice the supplier has voided, passed unnoticed, means finance pays or
// books against an invoice that does not hold up.
func Run(line string, ours InvoiceView, theirs SupplierInvoice) []Check {
	return nil
}
