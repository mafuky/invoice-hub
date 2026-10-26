//go:build integration

package store_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/mafuky/invoice-hub/internal/settlement/verify"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/settlement/domain"
	"github.com/mafuky/invoice-hub/internal/settlement/store"
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
	rows, err := db.Query(`SELECT from_status, to_status FROM state_log WHERE entity = ? AND entity_id = ? ORDER BY id`, store.EntitySettlement, id)
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
	forEachStore(t, func(t *testing.T, db *sql.DB, repo store.Store) {
		ctx := context.Background()
		customer := fmt.Sprintf("CUST-%08d", rand.IntN(100_000_000))

		tx := begin(t, db)
		first, err := repo.FindOrCreateSettlement(ctx, tx, "2026-10", customer, "test")
		if err != nil {
			t.Fatal(err)
		}
		if first.Status != domain.SettlementPendingSettle || first.Version != 1 || first.Amount.String() != "0.00" {
			t.Fatalf("new settlement: %+v", first)
		}
		again, err := repo.FindOrCreateSettlement(ctx, tx, "2026-10", customer, "test")
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
	})
}

func TestTransitionSettlement_StaleVersionChangesNothing(t *testing.T) {
	forEachStore(t, func(t *testing.T, db *sql.DB, repo store.Store) {
		ctx := context.Background()
		customer := fmt.Sprintf("CUST-%08d", rand.IntN(100_000_000))

		tx := begin(t, db)
		s, err := repo.FindOrCreateSettlement(ctx, tx, "2026-10", customer, "test")
		if err != nil {
			t.Fatal(err)
		}
		if applied, err := repo.TransitionSettlement(ctx, tx, s.ID, domain.SettlementPendingSettle, domain.SettlementCalculated, 0, "test"); err != nil || applied {
			t.Fatalf("zero stale version: applied=%v err=%v", applied, err)
		}
		applied, err := repo.TransitionSettlement(ctx, tx, s.ID, domain.SettlementPendingSettle, domain.SettlementCalculated, s.Version+5, "test")
		if err != nil || applied {
			t.Fatalf("stale version: applied=%v err=%v", applied, err)
		}
		applied, err = repo.TransitionSettlement(ctx, tx, s.ID, domain.SettlementCalculated, domain.SettlementPendingReview, s.Version, "test")
		if err != nil || applied {
			t.Fatalf("wrong source state: applied=%v err=%v", applied, err)
		}
		applied, err = repo.TransitionSettlement(ctx, tx, s.ID, domain.SettlementPendingSettle, domain.SettlementCalculated, s.Version, "test")
		if err != nil || !applied {
			t.Fatalf("fresh version: applied=%v err=%v", applied, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		got, err := repo.GetSettlement(ctx, s.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != domain.SettlementCalculated || got.Version != s.Version+1 {
			t.Fatalf("settlement: status=%s version=%d", got.Status, got.Version)
		}
		if rows := stateLog(t, db, s.ID); len(rows) != 3 {
			t.Fatalf("state_log = %v, want 3 rows (refused moves write none)", rows)
		}
	})
}

func TestInsertItem_SameInvoiceIsNotInsertedTwice(t *testing.T) {
	forEachStore(t, func(t *testing.T, db *sql.DB, repo store.Store) {
		ctx := context.Background()
		amount, _ := money.Parse("100.10")
		tx := begin(t, db)
		customer := fmt.Sprintf("CUST-%08d", rand.IntN(100_000_000))
		first, err := repo.FindOrCreateSettlement(ctx, tx, "2026-10", customer, "test")
		if err != nil {
			t.Fatal(err)
		}
		second, err := repo.FindOrCreateSettlement(ctx, tx, "2026-11", customer, "test")
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		it := store.Item{SettlementID: first.ID, InvoiceID: 1_000_000 + rand.Int64N(1<<40), ProductLine: "TRAIN", Amount: amount}

		if ok, err := repo.InsertItem(ctx, db, it); err != nil || !ok {
			t.Fatalf("first insert: inserted=%v err=%v", ok, err)
		}
		it.SettlementID = second.ID
		if ok, err := repo.InsertItem(ctx, db, it); err != nil || ok {
			t.Fatalf("second insert for the same invoice: inserted=%v err=%v", ok, err)
		}
		if exists, err := repo.HasItem(ctx, db, it.InvoiceID); err != nil || !exists {
			t.Fatalf("existing item: %v %v", exists, err)
		}
		if exists, err := repo.HasItem(ctx, db, -1); err != nil || exists {
			t.Fatalf("absent item: %v %v", exists, err)
		}
		total, err := repo.SumItems(ctx, db, first.ID)
		if err != nil || total.String() != "100.10" {
			t.Fatalf("sum = %s err=%v", total, err)
		}
		if empty, err := repo.SumItems(ctx, db, second.ID); err != nil || empty.String() != "0.00" {
			t.Fatalf("empty sum = %s err=%v", empty, err)
		}
	})
}

func TestFindOrCreateSettlement_LocksExistingRow(t *testing.T) {
	forEachStore(t, func(t *testing.T, db *sql.DB, repo store.Store) {
		ctx := context.Background()
		customer := fmt.Sprintf("CUST-%08d", rand.IntN(100_000_000))
		seed := begin(t, db)
		s, err := repo.FindOrCreateSettlement(ctx, seed, "2026-10", customer, "test")
		if err != nil {
			t.Fatal(err)
		}
		if err := seed.Commit(); err != nil {
			t.Fatal(err)
		}
		locked := begin(t, db)
		if _, err := repo.FindOrCreateSettlement(ctx, locked, "2026-10", customer, "test"); err != nil {
			t.Fatal(err)
		}
		other := begin(t, db)
		if _, err := other.ExecContext(ctx, "SET SESSION innodb_lock_wait_timeout = 1"); err != nil {
			t.Fatal(err)
		}
		blockedCtx, cancel := context.WithTimeout(ctx, arrivalTimeout)
		defer cancel()
		_, err = repo.TransitionSettlement(blockedCtx, other, s.ID, domain.SettlementPendingSettle, domain.SettlementCalculated, s.Version, "test")
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
		applied, err := repo.TransitionSettlement(ctx, freed, s.ID, domain.SettlementPendingSettle, domain.SettlementCalculated, s.Version, "test")
		if err != nil || !applied {
			t.Fatalf("calculation after unlock: applied=%v err=%v", applied, err)
		}
		if err := freed.Commit(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestFindOrCreateOpen_RollsPastClosedPeriods(t *testing.T) {
	forEachStore(t, func(t *testing.T, db *sql.DB, repo store.Store) {
		ctx := context.Background()
		customer := fmt.Sprintf("CUST-%08d", rand.IntN(100_000_000))
		tx := begin(t, db)
		for _, period := range []string{"2026-11", "2026-12"} {
			s, err := repo.FindOrCreateSettlement(ctx, tx, period, customer, "test")
			if err != nil {
				t.Fatal(err)
			}
			applied, err := repo.TransitionSettlement(ctx, tx, s.ID, domain.SettlementPendingSettle, domain.SettlementCalculated, s.Version, "test")
			if err != nil || !applied {
				t.Fatalf("close %s: applied=%v err=%v", period, applied, err)
			}
		}
		open, err := repo.FindOrCreateOpen(ctx, tx, customer, "2026-11")
		if err != nil {
			t.Fatal(err)
		}
		if open.Period != "2027-01" || open.CustomerID != customer || open.Status != domain.SettlementPendingSettle || open.Version != 1 {
			t.Fatalf("open settlement = %+v", open)
		}
		again, err := repo.FindOrCreateOpen(ctx, tx, customer, "2026-11")
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
	})
}

func TestSettlementHelpers_SnapshotConflictMappingAndRollback(t *testing.T) {
	forEachStore(t, func(t *testing.T, db *sql.DB, repo store.Store) {
		ctx := context.Background()
		stale := begin(t, db)
		var n int
		if err := stale.QueryRow("SELECT COUNT(*) FROM settlements").Scan(&n); err != nil {
			t.Fatal(err)
		}
		first := begin(t, db)
		s, err := repo.FindOrCreateSettlement(ctx, first, "2026-12", "CUST-TEST-MAPPING", "test")
		if err != nil {
			t.Fatal(err)
		}
		if err := first.Commit(); err != nil {
			t.Fatal(err)
		}
		found, err := repo.FindOrCreateSettlement(ctx, stale, "2026-12", "CUST-TEST-MAPPING", "test")
		if err != nil || found.ID != s.ID {
			t.Fatalf("stale snapshot duplicate re-read: %+v %v", found, err)
		}
		if err := stale.Commit(); err != nil {
			t.Fatal(err)
		}
		if len(stateLog(t, db, s.ID)) != 2 {
			t.Fatal("duplicate creation wrote state log")
		}
		amount, err := money.Parse("9999999999999999.99")
		if err != nil {
			t.Fatal(err)
		}
		tx := begin(t, db)
		if err := repo.SetAmount(ctx, tx, s.ID, amount); err != nil {
			t.Fatal(err)
		}
		if err := repo.SetRejectReason(ctx, tx, s.ID, "example correction"); err != nil {
			t.Fatal(err)
		}
		checks := []verify.Check{{Name: "amount", Passed: true}}
		if ok, err := repo.InsertItem(ctx, tx, store.Item{SettlementID: s.ID, InvoiceID: 101, ProductLine: "TRAIN", Amount: amount, Verified: true, Checks: checks}); err != nil || !ok {
			t.Fatalf("item: %v %v", ok, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		got, err := repo.GetSettlement(ctx, s.ID)
		if err != nil || got.Amount.String() != amount.String() || got.RejectReason != "example correction" || got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
			t.Fatalf("mapping: %+v %v", got, err)
		}
		rows, err := repo.ListSettlements(ctx, string(domain.SettlementPendingSettle))
		if err != nil || len(rows) != 1 || rows[0].ID != s.ID {
			t.Fatalf("list: %+v %v", rows, err)
		}
		items, err := repo.ListItems(ctx, s.ID)
		if err != nil || len(items) != 1 || !items[0].Verified || items[0].Amount.String() != amount.String() || !reflect.DeepEqual(items[0].Checks, checks) {
			t.Fatalf("items: %+v %v", items, err)
		}
		total, err := repo.SumItems(ctx, db, s.ID)
		if err != nil || total.String() != amount.String() {
			t.Fatalf("sum: %s %v", total, err)
		}
		tx = begin(t, db)
		if ok, err := repo.TransitionSettlement(ctx, tx, s.ID, got.Status, domain.SettlementCalculated, got.Version, "rollback"); err != nil || !ok {
			t.Fatalf("transition: %v %v", ok, err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		after, err := repo.GetSettlement(ctx, s.ID)
		if err != nil || after.Status != got.Status || after.Version != got.Version || len(stateLog(t, db, s.ID)) != 2 {
			t.Fatalf("rollback: %+v %v", after, err)
		}
		if _, err := repo.GetSettlement(ctx, -1); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("missing: %v", err)
		}
		tx = begin(t, db)
		if _, err := repo.FindOrCreateOpen(ctx, tx, "CUST-TEST-MAPPING", "invalid"); err == nil {
			t.Fatal("invalid period accepted")
		}
	})
}
