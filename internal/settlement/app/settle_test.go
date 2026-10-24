//go:build lesson && integration

package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
	"github.com/mafuky/invoice-hub/internal/settlement/domain"
	"github.com/mafuky/invoice-hub/internal/settlement/store"
	"github.com/mafuky/invoice-hub/internal/settlement/verify"
)

// matchingSupplier answers every lookup with a record that agrees with ours.
type matchingSupplier struct {
	amount money.Amount
	title  string
}

func (m matchingSupplier) Lookup(context.Context, string, string) (verify.SupplierInvoice, error) {
	return verify.SupplierInvoice{Found: true, Amount: m.amount, Title: m.title, Status: "issued"}, nil
}

func amt(t *testing.T, s string) money.Amount {
	t.Helper()
	a, err := money.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func newCustomerID() string { return fmt.Sprintf("CUST-%08d", rand.IntN(100_000_000)) }

func newInvoiceID() int64 { return 1_000_000 + rand.Int64N(1<<40) }

// seedSettlement opens a pending_settle settlement with one item per amount.
func seedSettlement(t *testing.T, db *sql.DB, repo *store.Repo, amounts ...string) (settlementID int64, invoiceIDs []int64) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	s, err := repo.FindOrCreateSettlement(ctx, tx, "2026-10", newCustomerID(), "test.seed")
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range amounts {
		id := newInvoiceID()
		ok, err := repo.InsertItem(ctx, tx, store.Item{
			SettlementID: s.ID, InvoiceID: id, ProductLine: "TRAIN", Amount: amt(t, a), Verified: true,
		})
		if err != nil || !ok {
			t.Fatalf("seed item: inserted=%v err=%v", ok, err)
		}
		invoiceIDs = append(invoiceIDs, id)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return s.ID, invoiceIDs
}

func transitions(t *testing.T, db *sql.DB, settlementID int64) []string {
	t.Helper()
	rows, err := db.Query(`SELECT from_status, to_status FROM state_log WHERE entity = ? AND entity_id = ? ORDER BY id`,
		store.EntitySettlement, settlementID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var from, to string
		if err := rows.Scan(&from, &to); err != nil {
			t.Fatal(err)
		}
		out = append(out, from+">"+to)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestRejectThenRecalculate_AmountReflectsChangedItems(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "settlement")
	repo := store.NewRepo(db)
	svc := NewService(db, repo, nil)
	id, invoices := seedSettlement(t, db, repo, "100.10", "200.20")

	check := func(wantStatus domain.SettlementStatus, wantAmount string) {
		t.Helper()
		s, err := repo.GetSettlement(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if s.Status != wantStatus {
			t.Fatalf("status = %s, want %s", s.Status, wantStatus)
		}
		if wantAmount != "" && s.Amount.String() != wantAmount {
			t.Fatalf("amount = %s, want %s", s.Amount, wantAmount)
		}
	}

	if err := svc.Calculate(ctx, id); err != nil {
		t.Fatalf("calculate: %v", err)
	}
	check(domain.SettlementCalculated, "300.30")
	if err := svc.SubmitReview(ctx, id); err != nil {
		t.Fatalf("submit review: %v", err)
	}
	check(domain.SettlementPendingReview, "300.30")
	if err := svc.Reject(ctx, id, "金额有误"); err != nil {
		t.Fatalf("reject: %v", err)
	}
	check(domain.SettlementPendingSettle, "")
	s, err := repo.GetSettlement(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if s.RejectReason != "金额有误" {
		t.Fatalf("reject_reason = %q", s.RejectReason)
	}

	// The reviewer's correction: the first item was really 150.00.
	if _, err := db.Exec(`UPDATE settlement_items SET amount = ? WHERE invoice_id = ?`, "150.00", invoices[0]); err != nil {
		t.Fatal(err)
	}
	if err := svc.Calculate(ctx, id); err != nil {
		t.Fatalf("recalculate: %v", err)
	}
	check(domain.SettlementCalculated, "250.10")
	if err := svc.SubmitReview(ctx, id); err != nil {
		t.Fatalf("second submit review: %v", err)
	}
	if err := svc.Approve(ctx, id); err != nil {
		t.Fatalf("approve: %v", err)
	}
	check(domain.SettlementPendingPayment, "250.10")
	if err := svc.Pay(ctx, id); err != nil {
		t.Fatalf("pay: %v", err)
	}
	check(domain.SettlementPaid, "250.10")

	want := []string{
		">created",
		"created>pending_settle",
		"pending_settle>calculated",
		"calculated>pending_review",
		"pending_review>rejected",
		"rejected>pending_settle",
		"pending_settle>calculated",
		"calculated>pending_review",
		"pending_review>pending_payment",
		"pending_payment>paid",
	}
	if got := transitions(t, db, id); !slices.Equal(got, want) {
		t.Fatalf("state_log = %v\nwant       %v", got, want)
	}
	final, err := repo.GetSettlement(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if final.Version != len(want)-1 {
		t.Fatalf("version = %d, want %d (one per move)", final.Version, len(want)-1)
	}
}

func TestSettlementMoves_RefusedOutOfOrder(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "settlement")
	repo := store.NewRepo(db)
	svc := NewService(db, repo, nil)
	id, _ := seedSettlement(t, db, repo, "100.10")

	// A settlement that was never calculated cannot be approved or paid.
	for name, err := range map[string]error{
		"submit review": svc.SubmitReview(ctx, id),
		"approve":       svc.Approve(ctx, id),
		"reject":        svc.Reject(ctx, id, "too early"),
		"pay":           svc.Pay(ctx, id),
	} {
		if !errors.Is(err, domain.ErrRefused) {
			t.Errorf("%s on pending_settle: err = %v, want ErrRefused", name, err)
		}
	}
	if err := svc.Calculate(ctx, id); err != nil {
		t.Fatal(err)
	}
	// Calculating again would move the total under a statement already calculated.
	if err := svc.Calculate(ctx, id); !errors.Is(err, domain.ErrRefused) {
		t.Errorf("second calculate: err = %v, want ErrRefused", err)
	}
	if err := svc.Calculate(ctx, 987654321); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("calculate unknown id: err = %v, want ErrNotFound", err)
	}
	if got := transitions(t, db, id); len(got) != 3 {
		t.Errorf("refused moves left state_log rows: %v", got)
	}
}

func deliveredEvent(t *testing.T, invoiceID int64, customerID string) event.Envelope {
	t.Helper()
	env, err := event.New(mq.InvoiceDelivered, fmt.Sprint(invoiceID), 3, event.InvoiceDelivered{
		InvoiceID: invoiceID, InvoiceNo: fmt.Sprintf("RAIL-A-%08d", invoiceID%100_000_000),
		CustomerID: customerID, ProductLine: "TRAIN", Title: "Example Travel Co., Ltd.",
		Amount:      amt(t, "100.10"),
		DeliveredAt: time.Date(2026, 10, 23, 9, 30, 0, 0, time.UTC),
	}, "trace-settlement-0001")
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func count(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestOnInvoiceDelivered_DuplicateAddsOnce(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "settlement")
	repo := store.NewRepo(db)
	svc := NewService(db, repo, matchingSupplier{amount: amt(t, "100.10"), title: "Example Travel Co., Ltd."})

	invoiceID, customerID := newInvoiceID(), newCustomerID()
	env := deliveredEvent(t, invoiceID, customerID)
	for i := 0; i < 2; i++ {
		if err := svc.OnInvoiceDelivered(ctx, env); err != nil {
			t.Fatalf("delivery %d of the same event: %v", i+1, err)
		}
	}
	// The same invoice announced again under a new event id is absorbed by the unique item.
	again := deliveredEvent(t, invoiceID, customerID)
	if err := svc.OnInvoiceDelivered(ctx, again); err != nil {
		t.Fatalf("redelivery under a new event id: %v", err)
	}

	if n := count(t, db, `SELECT COUNT(*) FROM settlement_items WHERE invoice_id = ?`, invoiceID); n != 1 {
		t.Fatalf("items for the invoice = %d, want 1", n)
	}
	added := count(t, db, `SELECT COUNT(*) FROM outbox WHERE routing_key = ? AND body->>'$.payload.invoice_id' = ?`,
		mq.SettlementItemAdded, fmt.Sprint(invoiceID))
	if added != 1 {
		t.Fatalf("settlement.item.added events = %d, want 1", added)
	}

	var status string
	var n int
	err := db.QueryRow(`SELECT COUNT(*), MAX(status) FROM settlements WHERE period = ? AND customer_id = ?`, "2026-10", customerID).Scan(&n, &status)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || status != string(domain.SettlementPendingSettle) {
		t.Fatalf("settlements for the customer in 2026-10: count=%d status=%q, want one pending_settle", n, status)
	}
}
