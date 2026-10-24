// Implemented in lesson 7.

package domain

// CanTo reports whether a settlement may move from s to next according to SettlementTransitions.
//
// Invariant: only moves listed in the table are allowed; a paid settlement is
// final, a settlement cannot be paid before it was reviewed, and a rejected
// one can only go back to pending_settle.
//
// If a settlement could jump from calculated straight to paid, a statement
// nobody reviewed would be paid out.
func (s SettlementStatus) CanTo(next SettlementStatus) bool { return false }
