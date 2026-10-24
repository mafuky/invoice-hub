package domain

import "testing"

func TestSettlementTransitions_Shape(t *testing.T) {
	if _, ok := SettlementTransitions[SettlementPaid]; ok {
		t.Error("paid is final and must have no outgoing moves")
	}
	review := SettlementTransitions[SettlementPendingReview]
	if len(review) != 2 {
		t.Fatalf("pending_review moves = %v, want approve and reject", review)
	}
	for from, tos := range SettlementTransitions {
		if !KnownSettlementStatus(from) {
			t.Errorf("unknown source status %q", from)
		}
		for _, to := range tos {
			if !KnownSettlementStatus(to) {
				t.Errorf("%s -> unknown status %q", from, to)
			}
		}
	}
	if KnownSettlementStatus("closed") {
		t.Error("closed is not a settlement status")
	}
}
