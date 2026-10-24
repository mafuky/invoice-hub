// Package domain holds the settlement states and the allowed moves between them.
package domain

import (
	"errors"
	"time"

	"github.com/mafuky/invoice-hub/internal/platform/money"
)

var (
	// ErrNotFound is returned when a settlement does not exist.
	ErrNotFound = errors.New("not found")
	// ErrRefused is returned when a settlement is not in a state that allows
	// the requested move, or it changed under the caller.
	ErrRefused = errors.New("transition refused")
)

type SettlementStatus string

const (
	SettlementCreated        SettlementStatus = "created"
	SettlementPendingSettle  SettlementStatus = "pending_settle"
	SettlementCalculated     SettlementStatus = "calculated"
	SettlementPendingReview  SettlementStatus = "pending_review"
	SettlementRejected       SettlementStatus = "rejected"
	SettlementPendingPayment SettlementStatus = "pending_payment"
	SettlementPaid           SettlementStatus = "paid"
)

var SettlementTransitions = map[SettlementStatus][]SettlementStatus{
	SettlementCreated:        {SettlementPendingSettle},
	SettlementPendingSettle:  {SettlementCalculated},
	SettlementCalculated:     {SettlementPendingReview},
	SettlementPendingReview:  {SettlementPendingPayment, SettlementRejected},
	SettlementRejected:       {SettlementPendingSettle},
	SettlementPendingPayment: {SettlementPaid},
}

// KnownSettlementStatus reports whether s is one of the settlement states.
func KnownSettlementStatus(s SettlementStatus) bool {
	switch s {
	case SettlementCreated, SettlementPendingSettle, SettlementCalculated, SettlementPendingReview,
		SettlementRejected, SettlementPendingPayment, SettlementPaid:
		return true
	}
	return false
}

// Settlement is the monthly statement of one customer.
type Settlement struct {
	ID           int64
	Period       string
	CustomerID   string
	Amount       money.Amount
	Status       SettlementStatus
	Version      int
	RejectReason string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}
