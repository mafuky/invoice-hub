//go:build integration

package store

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/mafuky/invoice-hub/internal/platform/testenv"
)

func TestApplyOutcome_OnlyNewerEventSeqApplies(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "provider")
	repo := NewRepo(db)
	taskID := int64(rand.IntN(1_000_000) + 1)
	no := fmt.Sprintf("AIR-A-ORD-%08x", rand.Uint32())
	if _, err := repo.InsertOrder(ctx, db, NewOrder{TaskID: taskID, InvoiceID: taskID, Supplier: "air-a", SupplierOrderNo: no}); err != nil {
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
	if !apply(0, 2, StatusSucceeded) {
		t.Fatal("seq 2 on a fresh order was not applied")
	}
	if apply(0, 3, StatusFailed) {
		t.Fatal("update at a stale version was applied")
	}
	if apply(1, 1, StatusFailed) {
		t.Fatal("older event_seq was applied")
	}
	if apply(1, 2, StatusFailed) {
		t.Fatal("equal event_seq was applied")
	}
	got, _ := repo.GetByTask(ctx, db, taskID)
	if got.Status != StatusSucceeded || got.LastEventSeq != 2 || got.Version != 1 {
		t.Fatalf("order is %s seq %d version %d, want succeeded seq 2 version 1", got.Status, got.LastEventSeq, got.Version)
	}
}
