//go:build lesson

package domain

import "testing"

func TestCanTo_SettlementTable(t *testing.T) {
	cases := []struct {
		from, to SettlementStatus
		want     bool
	}{
		{SettlementCreated, SettlementPendingSettle, true},
		{SettlementPendingSettle, SettlementCalculated, true},
		{SettlementCalculated, SettlementPendingReview, true},
		{SettlementPendingReview, SettlementPendingPayment, true},
		{SettlementPendingReview, SettlementRejected, true},
		{SettlementRejected, SettlementPendingSettle, true},
		{SettlementPendingPayment, SettlementPaid, true},
		{SettlementCalculated, SettlementPaid, false},
		{SettlementPendingSettle, SettlementPendingReview, false},
		{SettlementRejected, SettlementCalculated, false},
		{SettlementPendingPayment, SettlementPendingReview, false},
		{SettlementCalculated, SettlementCalculated, false},
	}
	for _, c := range cases {
		if got := c.from.CanTo(c.to); got != c.want {
			t.Errorf("settlement %s -> %s: got %v, want %v", c.from, c.to, got, c.want)
		}
	}
	all := []SettlementStatus{SettlementCreated, SettlementPendingSettle, SettlementCalculated, SettlementPendingReview,
		SettlementRejected, SettlementPendingPayment, SettlementPaid}
	for _, to := range all {
		if SettlementPaid.CanTo(to) {
			t.Errorf("paid settlement must not move to %s", to)
		}
	}
}
