//go:build integration

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
	"github.com/mafuky/invoice-hub/internal/settlement/domain"
)

const arrivalTimeout = 60 * time.Second

func begin(t *testing.T, db *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}

func stateLog(t *testing.T, db *sql.DB, id int64) []string {
	t.Helper()
	rows, err := db.Query(`SELECT from_status, to_status FROM state_log WHERE entity = ? AND entity_id = ? ORDER BY id`, EntitySettlement, id)
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
	return out
}

func TestFindOrCreateSettlement_SecondCallReturnsSameRow(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "settlement")
	r := NewRepo(db)
	customer := fmt.Sprintf("CUST-%08d", rand.IntN(100_000_000))

	tx := begin(t, db)
	first, err := r.FindOrCreateSettlement(ctx, tx, "2026-10", customer, "test")
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != domain.SettlementPendingSettle || first.Version != 1 || first.Amount.String() != "0.00" {
		t.Fatalf("new settlement: %+v", first)
	}
	again, err := r.FindOrCreateSettlement(ctx, tx, "2026-10", customer, "test")
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != first.ID {
		t.Fatalf("second call created settlement %d, want %d", again.ID, first.ID)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := stateLog(t, db, first.ID); len(got) != 2 || got[0] != ">created" || got[1] != "created>pending_settle" {
		t.Fatalf("state_log = %v", got)
	}
}

func TestTransitionSettlement_StaleVersionChangesNothing(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "settlement")
	r := NewRepo(db)
	customer := fmt.Sprintf("CUST-%08d", rand.IntN(100_000_000))

	tx := begin(t, db)
	s, err := r.FindOrCreateSettlement(ctx, tx, "2026-10", customer, "test")
	if err != nil {
		t.Fatal(err)
	}
	applied, err := r.TransitionSettlement(ctx, tx, s.ID, domain.SettlementPendingSettle, domain.SettlementCalculated, s.Version+5, "test")
	if err != nil || applied {
		t.Fatalf("stale version: applied=%v err=%v", applied, err)
	}
	applied, err = r.TransitionSettlement(ctx, tx, s.ID, domain.SettlementCalculated, domain.SettlementPendingReview, s.Version, "test")
	if err != nil || applied {
		t.Fatalf("wrong source state: applied=%v err=%v", applied, err)
	}
	applied, err = r.TransitionSettlement(ctx, tx, s.ID, domain.SettlementPendingSettle, domain.SettlementCalculated, s.Version, "test")
	if err != nil || !applied {
		t.Fatalf("fresh version: applied=%v err=%v", applied, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	got, err := r.GetSettlement(ctx, s.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.SettlementCalculated || got.Version != s.Version+1 {
		t.Fatalf("settlement: status=%s version=%d", got.Status, got.Version)
	}
	if rows := stateLog(t, db, s.ID); len(rows) != 3 {
		t.Fatalf("state_log = %v, want 3 rows (refused moves write none)", rows)
	}
}

func TestInsertItem_SameInvoiceIsNotInsertedTwice(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "settlement")
	r := NewRepo(db)
	amount, _ := money.Parse("100.10")
	tx := begin(t, db)
	customer := fmt.Sprintf("CUST-%08d", rand.IntN(100_000_000))
	first, err := r.FindOrCreateSettlement(ctx, tx, "2026-10", customer, "test")
	if err != nil {
		t.Fatal(err)
	}
	second, err := r.FindOrCreateSettlement(ctx, tx, "2026-11", customer, "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	it := Item{SettlementID: first.ID, InvoiceID: 1_000_000 + rand.Int64N(1<<40), ProductLine: "TRAIN", Amount: amount}

	if ok, err := r.InsertItem(ctx, db, it); err != nil || !ok {
		t.Fatalf("first insert: inserted=%v err=%v", ok, err)
	}
	it.SettlementID = second.ID
	if ok, err := r.InsertItem(ctx, db, it); err != nil || ok {
		t.Fatalf("second insert for the same invoice: inserted=%v err=%v", ok, err)
	}
	total, err := r.SumItems(ctx, db, first.ID)
	if err != nil || total.String() != "100.10" {
		t.Fatalf("sum = %s err=%v", total, err)
	}
	if empty, err := r.SumItems(ctx, db, second.ID); err != nil || empty.String() != "0.00" {
		t.Fatalf("empty sum = %s err=%v", empty, err)
	}
}

func TestFindOrCreateSettlement_LocksExistingRow(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "settlement")
	r := NewRepo(db)
	customer := fmt.Sprintf("CUST-%08d", rand.IntN(100_000_000))
	seed := begin(t, db)
	s, err := r.FindOrCreateSettlement(ctx, seed, "2026-10", customer, "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.Commit(); err != nil {
		t.Fatal(err)
	}
	locked := begin(t, db)
	if _, err := r.FindOrCreateSettlement(ctx, locked, "2026-10", customer, "test"); err != nil {
		t.Fatal(err)
	}
	other := begin(t, db)
	blockedCtx, cancel := context.WithTimeout(ctx, arrivalTimeout)
	defer cancel()
	_, err = r.TransitionSettlement(blockedCtx, other, s.ID, domain.SettlementPendingSettle, domain.SettlementCalculated, s.Version, "test")
	var lockErr *mysql.MySQLError
	lockTimedOut := errors.As(err, &lockErr) && lockErr.Number == 1205
	if err == nil || (!lockTimedOut && !errors.Is(blockedCtx.Err(), context.DeadlineExceeded)) {
		t.Fatalf("concurrent calculation was not blocked by the row lock: %v", err)
	}
	_ = other.Rollback()
	if err := locked.Commit(); err != nil {
		t.Fatal(err)
	}
	freed := begin(t, db)
	applied, err := r.TransitionSettlement(ctx, freed, s.ID, domain.SettlementPendingSettle, domain.SettlementCalculated, s.Version, "test")
	if err != nil || !applied {
		t.Fatalf("calculation after unlock: applied=%v err=%v", applied, err)
	}
	if err := freed.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestFindOrCreateOpen_RollsPastClosedPeriods(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "settlement")
	r := NewRepo(db)
	customer := fmt.Sprintf("CUST-%08d", rand.IntN(100_000_000))
	tx := begin(t, db)
	for _, period := range []string{"2026-11", "2026-12"} {
		s, err := r.FindOrCreateSettlement(ctx, tx, period, customer, "test")
		if err != nil {
			t.Fatal(err)
		}
		applied, err := r.TransitionSettlement(ctx, tx, s.ID, domain.SettlementPendingSettle, domain.SettlementCalculated, s.Version, "test")
		if err != nil || !applied {
			t.Fatalf("close %s: applied=%v err=%v", period, applied, err)
		}
	}
	open, err := r.FindOrCreateOpen(ctx, tx, customer, "2026-11")
	if err != nil {
		t.Fatal(err)
	}
	if open.Period != "2027-01" || open.CustomerID != customer || open.Status != domain.SettlementPendingSettle || open.Version != 1 {
		t.Fatalf("open settlement = %+v", open)
	}
	again, err := r.FindOrCreateOpen(ctx, tx, customer, "2026-11")
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != open.ID {
		t.Fatalf("open settlement duplicated: %d != %d", again.ID, open.ID)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := stateLog(t, db, open.ID); len(got) != 2 || got[0] != ">created" || got[1] != "created>pending_settle" {
		t.Fatalf("open settlement state log = %v", got)
	}
}
