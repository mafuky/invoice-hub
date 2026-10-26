//go:build lesson && integration

package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/mafuky/invoice-hub/internal/platform/consume"
	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
	"github.com/mafuky/invoice-hub/internal/platform/outbox"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
	"github.com/mafuky/invoice-hub/internal/settlement/domain"
	"github.com/mafuky/invoice-hub/internal/settlement/store"
	"github.com/mafuky/invoice-hub/internal/settlement/store/gormstore"
	"github.com/mafuky/invoice-hub/internal/settlement/verify"
)

type lookupCall struct {
	line, invoiceNo string
}

type recordingSupplier struct {
	mu       sync.Mutex
	calls    []lookupCall
	result   verify.SupplierInvoice
	failures int
}

func (m *recordingSupplier) Lookup(_ context.Context, line, invoiceNo string) (verify.SupplierInvoice, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, lookupCall{line, invoiceNo})
	if m.failures > 0 {
		m.failures--
		return verify.SupplierInvoice{}, verify.ErrSupplier
	}
	return m.result, nil
}

func newSupplier(t *testing.T) *recordingSupplier {
	t.Helper()
	return &recordingSupplier{result: verify.SupplierInvoice{
		Found: true, Amount: amt(t, "100.10"), Title: "Example Travel Co., Ltd.", Status: "issued",
	}}
}

func (m *recordingSupplier) assertCalls(t *testing.T, events ...event.Envelope) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	want := make(map[lookupCall]int)
	for _, env := range events {
		var payload event.InvoiceDelivered
		if err := env.Decode(&payload); err != nil {
			t.Fatal(err)
		}
		want[lookupCall{payload.ProductLine, payload.InvoiceNo}]++
	}
	got := make(map[lookupCall]int)
	for _, call := range m.calls {
		got[call]++
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("supplier calls = %v, want %v", got, want)
	}
}

func amt(t *testing.T, s string) money.Amount {
	t.Helper()
	a, err := money.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

const arrivalTimeout = 60 * time.Second

func newCustomerID() string { return fmt.Sprintf("CUST-%08d", rand.IntN(100_000_000)) }

func newInvoiceID() int64 { return 1_000_000 + rand.Int64N(1<<40) }

// seedSettlement opens a pending_settle settlement with one item per amount.
func seedSettlement(t *testing.T, db *sql.DB, repo store.Store, amounts ...string) (settlementID int64, invoiceIDs []int64) {
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
	for _, impl := range []string{"sql", "gorm"} {
		t.Run(impl, func(t *testing.T) {
			ctx := context.Background()
			db := testenv.OpenMySQL(t, "settlement")
			repo := newRepo(t, db, impl)
			svc := NewService(db, repo, nil, WithPersistence(consume.ForImplementation(impl), outbox.ForImplementation(impl)))
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

			// The reviewer's correction: the second item was 150.00, not 200.20.
			if _, err := db.Exec(`UPDATE settlement_items SET amount = ? WHERE invoice_id = ?`, "150.00", invoices[1]); err != nil {
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
		})
	}
}

func TestSettlementMoves_RefusedOutOfOrder(t *testing.T) {
	for _, impl := range []string{"sql", "gorm"} {
		t.Run(impl, func(t *testing.T) {
			ctx := context.Background()
			db := testenv.OpenMySQL(t, "settlement")
			repo := newRepo(t, db, impl)
			svc := NewService(db, repo, nil, WithPersistence(consume.ForImplementation(impl), outbox.ForImplementation(impl)))
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
		})
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
	for _, impl := range []string{"sql", "gorm"} {
		t.Run(impl, func(t *testing.T) {
			ctx := context.Background()
			db := testenv.OpenMySQL(t, "settlement")
			repo := newRepo(t, db, impl)
			supplier := newSupplier(t)
			svc := NewService(db, repo, supplier, WithPersistence(consume.ForImplementation(impl), outbox.ForImplementation(impl)))

			invoiceID, customerID := newInvoiceID(), newCustomerID()
			env := deliveredEvent(t, invoiceID, customerID)
			for i := 0; i < 2; i++ {
				if err := svc.OnInvoiceDelivered(ctx, env); err != nil {
					t.Fatalf("delivery %d of the same event: %v", i+1, err)
				}
			}
			supplier.assertCalls(t, env)
			settlementID := assertDelivery(t, db, env, "2026-10")
			// The same invoice announced again under a new event id is absorbed by the unique item.
			again := deliveredEvent(t, invoiceID, customerID)
			if err := svc.OnInvoiceDelivered(ctx, again); err != nil {
				t.Fatalf("redelivery under a new event id: %v", err)
			}
			if got := assertDelivery(t, db, again, "2026-10"); got != settlementID {
				t.Fatalf("duplicate moved from settlement %d to %d", settlementID, got)
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
		})
	}
}

func assertDelivery(t *testing.T, db *sql.DB, env event.Envelope, period string) int64 {
	t.Helper()
	var invoice event.InvoiceDelivered
	if err := env.Decode(&invoice); err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, `SELECT COUNT(*) FROM settlement_items WHERE invoice_id = ?`, invoice.InvoiceID); n != 1 {
		t.Fatalf("invoice items = %d, want 1", n)
	}
	var settlementID int64
	var raw []byte
	if err := db.QueryRow(`SELECT settlement_id, COALESCE(checks, 'null') FROM settlement_items WHERE invoice_id = ?`, invoice.InvoiceID).
		Scan(&settlementID, &raw); err != nil {
		t.Fatal(err)
	}
	var checks []json.RawMessage
	if err := json.Unmarshal(raw, &checks); err != nil {
		t.Fatalf("checks JSON: %v", err)
	}
	if n := count(t, db, `SELECT COUNT(*) FROM settlements WHERE id = ? AND customer_id = ? AND period = ? AND status = ?`,
		settlementID, invoice.CustomerID, period, domain.SettlementPendingSettle); n != 1 {
		t.Fatalf("item is not in the customer's open %s settlement", period)
	}
	assertProcessed(t, db, env)
	rows, err := db.Query(`SELECT body FROM outbox WHERE routing_key = ? AND body->>'$.payload.invoice_id' = ?`,
		mq.SettlementItemAdded, fmt.Sprint(invoice.InvoiceID))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
		var body []byte
		if err := rows.Scan(&body); err != nil {
			t.Fatal(err)
		}
		var addedEnv event.Envelope
		if err := json.Unmarshal(body, &addedEnv); err != nil {
			t.Fatal(err)
		}
		var added event.ItemAdded
		if err := addedEnv.Decode(&added); err != nil {
			t.Fatal(err)
		}
		if addedEnv.Type != mq.SettlementItemAdded || added.InvoiceID != invoice.InvoiceID || added.SettlementID != settlementID {
			t.Errorf("ItemAdded = %+v, type = %q, want invoice %d / settlement %d", added, addedEnv.Type, invoice.InvoiceID, settlementID)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("ItemAdded rows = %d, want 1", n)
	}
	return settlementID
}

func assertProcessed(t *testing.T, db *sql.DB, env event.Envelope) {
	t.Helper()
	if n := count(t, db, `SELECT COUNT(*) FROM processed_events WHERE event_id = ? AND consumer = ?`, env.EventID, consumerName); n != 1 {
		t.Fatalf("processed_events for %s = %d, want 1", env.EventID, n)
	}
}

func TestOnInvoiceDelivered_VerificationAnomaly(t *testing.T) {
	for _, impl := range []string{"sql", "gorm"} {
		t.Run(impl, func(t *testing.T) {
			db := testenv.OpenMySQL(t, "settlement")
			supplier := newSupplier(t)
			supplier.result.Title = "Example Branch Co., Ltd."
			svc := NewService(db, newRepo(t, db, impl), supplier, WithPersistence(consume.ForImplementation(impl), outbox.ForImplementation(impl)))
			env := deliveredEvent(t, newInvoiceID(), newCustomerID())
			if err := svc.OnInvoiceDelivered(context.Background(), env); err != nil {
				t.Fatal(err)
			}
			supplier.assertCalls(t, env)
			assertDelivery(t, db, env, "2026-10")
		})
	}
}

// TestOnInvoiceDelivered_StoresVerificationChecks also requires lesson 8 (verify.Run).
func TestOnInvoiceDelivered_StoresVerificationChecks(t *testing.T) {
	for _, impl := range []string{"sql", "gorm"} {
		t.Run(impl, func(t *testing.T) {
			for _, tc := range []struct {
				name, title  string
				wantVerified bool
			}{
				{"matching", "Example Travel Co., Ltd.", true},
				{"title mismatch", "Example Branch Co., Ltd.", false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					db := testenv.OpenMySQL(t, "settlement")
					supplier := newSupplier(t)
					supplier.result.Title = tc.title
					svc := NewService(db, newRepo(t, db, impl), supplier, WithPersistence(consume.ForImplementation(impl), outbox.ForImplementation(impl)))
					invoiceID := newInvoiceID()
					env := deliveredEvent(t, invoiceID, newCustomerID())
					if err := svc.OnInvoiceDelivered(context.Background(), env); err != nil {
						t.Fatal(err)
					}
					supplier.assertCalls(t, env)
					assertDelivery(t, db, env, "2026-10")
					var verified bool
					var raw []byte
					if err := db.QueryRow(`SELECT verified, checks FROM settlement_items WHERE invoice_id = ?`, invoiceID).
						Scan(&verified, &raw); err != nil {
						t.Fatal(err)
					}
					var checks []verify.Check
					if err := json.Unmarshal(raw, &checks); err != nil {
						t.Fatalf("checks JSON: %v", err)
					}
					if len(checks) != 4 {
						t.Fatalf("checks = %+v, want four", checks)
					}
					names := map[string]bool{verify.CheckExists: true, verify.CheckAmount: true, verify.CheckTitle: true, verify.CheckStatus: true}
					allPassed := true
					for _, check := range checks {
						if !names[check.Name] {
							t.Fatalf("unknown or duplicate check: %q", check.Name)
						}
						delete(names, check.Name)
						wantPassed := tc.wantVerified || check.Name != verify.CheckTitle
						if check.Passed != wantPassed {
							t.Errorf("%s passed = %v, want %v", check.Name, check.Passed, wantPassed)
						}
						if !check.Passed && check.Detail == "" {
							t.Errorf("%s has no failure detail", check.Name)
						}
						allPassed = allPassed && check.Passed
					}
					if verified != allPassed || verified != tc.wantVerified {
						t.Errorf("verified = %v, all passed = %v, want %v", verified, allPassed, tc.wantVerified)
					}
				})
			}
		})
	}
}

func TestOnInvoiceDelivered_VerifyFailureRollsBackAndRetries(t *testing.T) {
	for _, impl := range []string{"sql", "gorm"} {
		t.Run(impl, func(t *testing.T) {
			db := testenv.OpenMySQL(t, "settlement")
			supplier := newSupplier(t)
			supplier.failures = 1
			svc := NewService(db, newRepo(t, db, impl), supplier, WithPersistence(consume.ForImplementation(impl), outbox.ForImplementation(impl)))
			invoiceID, customerID := newInvoiceID(), newCustomerID()
			env := deliveredEvent(t, invoiceID, customerID)
			if err := svc.OnInvoiceDelivered(context.Background(), env); !errors.Is(err, verify.ErrSupplier) {
				t.Fatalf("lookup failure = %v, want ErrSupplier", err)
			}
			supplier.assertCalls(t, env)
			for _, query := range []struct {
				sql string
				arg any
			}{
				{`SELECT COUNT(*) FROM settlement_items WHERE invoice_id = ?`, invoiceID},
				{`SELECT COUNT(*) FROM outbox WHERE body->>'$.payload.invoice_id' = ?`, fmt.Sprint(invoiceID)},
				{`SELECT COUNT(*) FROM processed_events WHERE event_id = ?`, env.EventID},
				{`SELECT COUNT(*) FROM settlements WHERE customer_id = ?`, customerID},
			} {
				if n := count(t, db, query.sql, query.arg); n != 0 {
					t.Errorf("rollback left %d rows: %s", n, query.sql)
				}
			}
			if err := svc.OnInvoiceDelivered(context.Background(), env); err != nil {
				t.Fatalf("retry: %v", err)
			}
			supplier.assertCalls(t, env, env)
			assertDelivery(t, db, env, "2026-10")
		})
	}
}

func TestOnInvoiceDelivered_ConcurrentInvoicesShareSettlement(t *testing.T) {
	for _, impl := range []string{"sql", "gorm"} {
		t.Run(impl, func(t *testing.T) {
			db := testenv.OpenMySQL(t, "settlement")
			supplier := newSupplier(t)
			svc := NewService(db, newRepo(t, db, impl), supplier, WithPersistence(consume.ForImplementation(impl), outbox.ForImplementation(impl)))
			customerID := newCustomerID()
			events := []event.Envelope{deliveredEvent(t, newInvoiceID(), customerID), deliveredEvent(t, newInvoiceID(), customerID)}
			ctx, cancel := context.WithTimeout(context.Background(), arrivalTimeout)
			defer cancel()
			start := make(chan struct{})
			errc := make(chan error, len(events))
			for _, env := range events {
				go func() { <-start; errc <- svc.OnInvoiceDelivered(ctx, env) }()
			}
			close(start)
			for range events {
				if err := <-errc; err != nil {
					t.Errorf("concurrent delivery: %v", err)
				}
			}
			if t.Failed() {
				return
			}
			supplier.assertCalls(t, events...)
			first := assertDelivery(t, db, events[0], "2026-10")
			second := assertDelivery(t, db, events[1], "2026-10")
			if first != second {
				t.Fatalf("different settlements: %d, %d", first, second)
			}
			if n := count(t, db, `SELECT COUNT(*) FROM settlements WHERE customer_id = ? AND period = ?`, customerID, "2026-10"); n != 1 {
				t.Errorf("settlements = %d, want 1", n)
			}
			if n := count(t, db, `SELECT COUNT(*) FROM settlement_items WHERE settlement_id = ?`, first); n != 2 {
				t.Errorf("items = %d, want 2", n)
			}
		})
	}
}

func TestOnInvoiceDelivered_UTCMonthBoundary(t *testing.T) {
	for _, impl := range []string{"sql", "gorm"} {
		t.Run(impl, func(t *testing.T) {
			db := testenv.OpenMySQL(t, "settlement")
			supplier := newSupplier(t)
			svc := NewService(db, newRepo(t, db, impl), supplier, WithPersistence(consume.ForImplementation(impl), outbox.ForImplementation(impl)))
			customerID := newCustomerID()
			var events []event.Envelope
			for _, tc := range []struct{ deliveredAt, period string }{
				{"2026-10-31T23:30:00Z", "2026-10"},
				{"2026-11-01T00:10:00Z", "2026-11"},
				{"2026-11-01T00:30:00+08:00", "2026-10"},
			} {
				env := deliveredEvent(t, newInvoiceID(), customerID)
				var payload event.InvoiceDelivered
				if err := env.Decode(&payload); err != nil {
					t.Fatal(err)
				}
				var err error
				payload.DeliveredAt, err = time.Parse(time.RFC3339, tc.deliveredAt)
				if err != nil {
					t.Fatal(err)
				}
				env.Payload, err = json.Marshal(payload)
				if err != nil {
					t.Fatal(err)
				}
				env.OccurredAt = time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
				if err := svc.OnInvoiceDelivered(context.Background(), env); err != nil {
					t.Fatal(err)
				}
				assertDelivery(t, db, env, tc.period)
				events = append(events, env)
			}
			supplier.assertCalls(t, events...)
			if n := count(t, db, `SELECT COUNT(*) FROM settlements WHERE customer_id = ?`, customerID); n != 2 {
				t.Errorf("settlements = %d, want 2", n)
			}
		})
	}
}

func TestOnInvoiceDelivered_LateInvoiceRollsToNextPeriod(t *testing.T) {
	for _, impl := range []string{"sql", "gorm"} {
		t.Run(impl, func(t *testing.T) {
			for _, status := range []domain.SettlementStatus{domain.SettlementCalculated, domain.SettlementPendingReview, domain.SettlementPaid} {
				t.Run(string(status), func(t *testing.T) {
					ctx := context.Background()
					db := testenv.OpenMySQL(t, "settlement")
					repo := newRepo(t, db, impl)
					supplier := newSupplier(t)
					svc := NewService(db, repo, supplier, WithPersistence(consume.ForImplementation(impl), outbox.ForImplementation(impl)))
					id, _ := seedSettlement(t, db, repo, "200.20")
					if err := svc.Calculate(ctx, id); err != nil {
						t.Fatal(err)
					}
					if status != domain.SettlementCalculated {
						if err := svc.SubmitReview(ctx, id); err != nil {
							t.Fatal(err)
						}
					}
					if status == domain.SettlementPaid {
						if err := svc.Approve(ctx, id); err != nil {
							t.Fatal(err)
						}
						if err := svc.Pay(ctx, id); err != nil {
							t.Fatal(err)
						}
					}
					before, err := repo.GetSettlement(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					items, err := repo.ListItems(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					env := deliveredEvent(t, newInvoiceID(), before.CustomerID)
					for range 2 {
						if err := svc.OnInvoiceDelivered(ctx, env); err != nil {
							t.Fatal(err)
						}
						novemberID := assertDelivery(t, db, env, "2026-11")
						if got := transitions(t, db, novemberID); !slices.Equal(got, []string{">created", "created>pending_settle"}) {
							t.Errorf("November state log = %v", got)
						}
						after, err := repo.GetSettlement(ctx, id)
						if err != nil {
							t.Fatal(err)
						}
						afterItems, err := repo.ListItems(ctx, id)
						if err != nil {
							t.Fatal(err)
						}
						if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(items, afterItems) {
							t.Fatalf("late delivery changed October: before=%+v after=%+v", before, after)
						}
						if n := count(t, db, `SELECT COUNT(*) FROM settlement_items WHERE settlement_id = ?`, novemberID); n != 1 {
							t.Errorf("November items = %d, want 1", n)
						}
					}
					supplier.assertCalls(t, env)
					if n := count(t, db, `SELECT COUNT(*) FROM settlements WHERE customer_id = ?`, before.CustomerID); n != 2 {
						t.Errorf("settlements after replay = %d, want 2", n)
					}
				})
			}
		})
	}
}

func newRepo(t *testing.T, db *sql.DB, impl string) store.Store {
	t.Helper()
	if impl == "gorm" {
		repo, err := gormstore.NewRepo(db)
		if err != nil {
			t.Fatal(err)
		}
		return repo
	}
	return store.NewRepo(db)
}
