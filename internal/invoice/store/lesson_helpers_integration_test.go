//go:build integration

package store_test

import (
	"context"
	"testing"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/invoice/store"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
)

func TestLessonLookups_UseCallerTransactionAndScope(t *testing.T) {
	forEachStore(t, func(t *testing.T, r *testRepo) {
		ctx := context.Background()
		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		amount, _ := money.Parse("100.10")
		inv, err := r.InsertInvoice(ctx, tx, store.NewInvoice{BizOrderNo: testenv.BizOrderNo(), ProductLine: "TRAIN", Title: "Example Travel Co., Ltd.", TaxNo: "91310000EXAMPLE001", Amount: amount, CustomerID: "CUST-0001"})
		if err != nil {
			t.Fatal(err)
		}
		first, err := r.InsertCommand(ctx, tx, inv, domain.KindIssue, "first")
		if err != nil {
			t.Fatal(err)
		}
		second, err := r.InsertCommand(ctx, tx, inv, domain.KindRedFlush, "second")
		if err != nil {
			t.Fatal(err)
		}
		task, err := r.InsertTask(ctx, tx, first, inv, "")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.InsertTask(ctx, tx, second, inv, "air-a"); err != nil {
			t.Fatal(err)
		}
		commands, err := r.ListCommandsForUpdate(ctx, tx, inv)
		if err != nil {
			t.Fatal(err)
		}
		if len(commands) != 2 || commands[0].ID != first || commands[1].ID != second || commands[0].Reason != "first" || commands[1].Kind != domain.KindRedFlush {
			t.Fatalf("commands=%+v", commands)
		}
		tasks, err := r.ListTasksForUpdate(ctx, tx, first)
		if err != nil {
			t.Fatal(err)
		}
		if len(tasks) != 1 || tasks[0].ID != task || tasks[0].Supplier != "" || tasks[0].Status != domain.TaskPending {
			t.Fatalf("tasks=%+v", tasks)
		}
		if empty, err := r.ListCommandsForUpdate(ctx, tx, inv+1); err != nil || len(empty) != 0 {
			t.Fatalf("unrelated commands=%+v err=%v", empty, err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		tx, err = r.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if rows, err := r.ListCommandsForUpdate(ctx, tx, inv); err != nil || len(rows) != 0 {
			t.Fatalf("commands survived rollback: %+v %v", rows, err)
		}
		if rows, err := r.ListTasksForUpdate(ctx, tx, first); err != nil || len(rows) != 0 {
			t.Fatalf("tasks survived rollback: %+v %v", rows, err)
		}
	})
}
