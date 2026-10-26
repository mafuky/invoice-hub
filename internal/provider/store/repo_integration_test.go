//go:build integration

package store_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"math/rand/v2"
	"testing"

	"github.com/mafuky/invoice-hub/internal/provider/store"
)

func TestApplyOutcome_OnlyNewerEventSeqApplies(t *testing.T) {
	forEachStore(t, func(t *testing.T, db *sql.DB, repo store.Store) {
		ctx := context.Background()
		taskID := int64(rand.IntN(1_000_000) + 1)
		no := fmt.Sprintf("AIR-A-ORD-%08x", rand.Uint32())
		if _, err := repo.InsertOrder(ctx, db, store.NewOrder{TaskID: taskID, InvoiceID: taskID, Supplier: "air-a", SupplierOrderNo: no}); err != nil {
			t.Fatal(err)
		}
		o, err := repo.GetByTask(ctx, db, taskID)
		if err != nil {
			t.Fatal(err)
		}

		apply := func(version int, seq int64, status string) bool {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			ok, err := repo.ApplyOutcome(ctx, tx, o.ID, version, seq, status, status, "")
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			return ok
		}
		if !apply(0, 2, store.StatusFailed) {
			t.Fatal("seq 2 on a fresh order was not applied")
		}
		if apply(0, 3, store.StatusFailed) {
			t.Fatal("update at a stale version was applied")
		}
		if apply(1, 1, store.StatusFailed) {
			t.Fatal("older event_seq was applied")
		}
		if apply(1, 2, store.StatusFailed) {
			t.Fatal("equal event_seq was applied")
		}
		got, _ := repo.GetByTask(ctx, db, taskID)
		if got.Status != store.StatusFailed || got.LastEventSeq != 2 || got.Version != 1 {
			t.Fatalf("order is %s seq %d version %d, want failed seq 2 version 1", got.Status, got.LastEventSeq, got.Version)
		}
		if !apply(1, 3, store.StatusSucceeded) {
			t.Fatal("newer success after failure was not applied")
		}
		if apply(2, 4, store.StatusFailed) {
			t.Fatal("succeeded order was rewritten by a newer failure")
		}

	})
}

func TestOrderHelpers_DuplicateKeysRollbackAndExclusions(t *testing.T) {
	forEachStore(t, func(t *testing.T, db *sql.DB, repo store.Store) {
		ctx := context.Background()
		input := store.NewOrder{TaskID: 10, InvoiceID: 20, Supplier: "air-a", SupplierOrderNo: "AIR-A-TEST-10"}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if ok, err := repo.InsertOrder(ctx, tx, input); err != nil || !ok {
			t.Fatalf("insert: %v %v", ok, err)
		}
		if _, err := repo.GetBySupplierOrderForUpdate(ctx, tx, input.Supplier, input.SupplierOrderNo); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.OrderByTask(ctx, 10); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("rollback: %v", err)
		}
		if ok, err := repo.InsertOrder(ctx, db, input); err != nil || !ok {
			t.Fatalf("insert: %v %v", ok, err)
		}
		input.SupplierOrderNo = "OTHER"
		if ok, err := repo.InsertOrder(ctx, db, input); err != nil || ok {
			t.Fatalf("task duplicate: %v %v", ok, err)
		}
		input.TaskID = 11
		input.SupplierOrderNo = "AIR-A-TEST-10"
		if ok, err := repo.InsertOrder(ctx, db, input); err == nil || ok {
			t.Fatalf("supplier-order collision swallowed: %v %v", ok, err)
		}
		o, err := repo.GetBySupplierOrder(ctx, db, "air-a", "AIR-A-TEST-10")
		if err != nil {
			t.Fatal(err)
		}
		if o.Outcome != "" || o.InvoiceNo != "" || o.Version != 0 || o.LastEventSeq != 0 {
			t.Fatalf("defaults: %+v", o)
		}
		excluded, err := repo.ExcludedSuppliers(ctx, db, 20, 11)
		if err != nil || len(excluded) != 1 || excluded[0] != "air-a" {
			t.Fatalf("older submitted exclusion: %v %v", excluded, err)
		}
		excluded, err = repo.ExcludedSuppliers(ctx, db, 20, 10)
		if err != nil || excluded != nil {
			t.Fatalf("current task excluded: %v %v", excluded, err)
		}
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		for i, st := range []string{store.StatusFailed, store.StatusSucceeded} {
			ok, err := repo.ApplyOutcome(ctx, tx, o.ID, i, int64(i+1), st, st, "INV-TEST-10")
			if err != nil || !ok {
				t.Fatalf("apply %s: %v %v", st, ok, err)
			}
		}
		if ok, err := repo.ApplyOutcome(ctx, tx, o.ID, 2, 3, store.StatusFailed, store.StatusFailed, ""); err != nil || ok {
			t.Fatalf("succeeded rewritten: %v %v", ok, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		o, err = repo.OrderByTask(ctx, 10)
		if err != nil || o.Status != store.StatusSucceeded || o.Version != 2 || o.InvoiceNo != "INV-TEST-10" {
			t.Fatalf("outcome: %+v %v", o, err)
		}
	})
}

func TestGetBySupplierOrderForUpdate_BlocksConcurrentOutcome(t *testing.T) {
	forEachStore(t, func(t *testing.T, db *sql.DB, repo store.Store) {
		ctx := context.Background()
		if _, err := repo.InsertOrder(ctx, db, store.NewOrder{TaskID: 30, InvoiceID: 40, Supplier: "air-a", SupplierOrderNo: "AIR-A-LOCK-30"}); err != nil {
			t.Fatal(err)
		}
		locked, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer locked.Rollback()
		order, err := repo.GetBySupplierOrderForUpdate(ctx, locked, "air-a", "AIR-A-LOCK-30")
		if err != nil {
			t.Fatal(err)
		}
		other, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer other.Rollback()
		if _, err := other.ExecContext(ctx, "SET SESSION innodb_lock_wait_timeout = 1"); err != nil {
			t.Fatal(err)
		}
		_, err = repo.ApplyOutcome(ctx, other, order.ID, 0, 1, store.StatusSucceeded, "SUCCEEDED", "INV-LOCK-30")
		var me *mysql.MySQLError
		if !errors.As(err, &me) || me.Number != 1205 {
			t.Fatalf("concurrent outcome was not blocked: %v", err)
		}
		if err := other.Rollback(); err != nil {
			t.Fatal(err)
		}
		if err := locked.Commit(); err != nil {
			t.Fatal(err)
		}
		freed, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer freed.Rollback()
		if applied, err := repo.ApplyOutcome(ctx, freed, order.ID, 0, 1, store.StatusSucceeded, "SUCCEEDED", "INV-LOCK-30"); err != nil || !applied {
			t.Fatalf("outcome after unlock: %v %v", applied, err)
		}
		if err := freed.Commit(); err != nil {
			t.Fatal(err)
		}
	})
}
