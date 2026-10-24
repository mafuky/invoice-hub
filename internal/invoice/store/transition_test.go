//go:build lesson && integration

package store

import (
	"context"
	"testing"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
)

func seedInvoice(t *testing.T, r *Repo) int64 {
	t.Helper()
	amt, err := money.Parse("1999.99")
	if err != nil {
		t.Fatal(err)
	}
	id, err := r.InsertInvoice(context.Background(), r.db, NewInvoice{
		BizOrderNo: testenv.BizOrderNo(), ProductLine: "FLIGHT", Title: "Example Travel Co., Ltd.",
		TaxNo: "91310000EXAMPLE001", Amount: amt, CustomerID: "CUST-0001",
	})
	if err != nil {
		t.Fatalf("seed invoice: %v", err)
	}
	return id
}

func stateLogRows(t *testing.T, q DBTX, entity string, id int64) int {
	t.Helper()
	var n int
	err := q.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM state_log WHERE entity = ? AND entity_id = ?`, entity, id).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestTransitionInvoice_VersionMismatch(t *testing.T) {
	ctx := context.Background()
	r := NewRepo(testenv.OpenMySQL(t, "invoice"))
	id := seedInvoice(t, r)

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	applied, err := r.TransitionInvoice(ctx, tx, id, domain.InvoiceApplying, domain.InvoiceIssuing, 7, "provider.task.submitted", "11111111-1111-1111-1111-111111111111")
	if err != nil {
		t.Fatalf("stale version is not an error: %v", err)
	}
	if applied {
		t.Fatal("transition applied with a stale version")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if n := stateLogRows(t, r.db, EntityInvoice, id); n != 0 {
		t.Fatalf("state_log has %d rows for a refused transition", n)
	}
	inv, err := r.GetInvoice(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if inv.Status != domain.InvoiceApplying || inv.Version != 0 {
		t.Fatalf("invoice changed: status=%s version=%d", inv.Status, inv.Version)
	}
}

func TestTransitionInvoice_AppliedWritesOneStateLogRow(t *testing.T) {
	ctx := context.Background()
	r := NewRepo(testenv.OpenMySQL(t, "invoice"))
	id := seedInvoice(t, r)

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	applied, err := r.TransitionInvoice(ctx, tx, id, domain.InvoiceApplying, domain.InvoiceIssuing, 0, "provider.task.submitted", "22222222-2222-2222-2222-222222222222")
	if err != nil || !applied {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if n := stateLogRows(t, r.db, EntityInvoice, id); n != 1 {
		t.Fatalf("state_log rows = %d, want 1", n)
	}
	inv, err := r.GetInvoice(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if inv.Status != domain.InvoiceIssuing || inv.Version != 1 {
		t.Fatalf("invoice after move: status=%s version=%d", inv.Status, inv.Version)
	}
}

func TestTransitionInvoice_IllegalJump_Refused(t *testing.T) {
	ctx := context.Background()
	r := NewRepo(testenv.OpenMySQL(t, "invoice"))
	id := seedInvoice(t, r)

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// delivery.invoice.delivered arriving before provider.task.succeeded
	applied, _ := r.TransitionInvoice(ctx, tx, id, domain.InvoiceApplying, domain.InvoiceDelivered, 0, "delivery.invoice.delivered", "33333333-3333-3333-3333-333333333333")
	if applied {
		t.Fatal("applying -> delivered must be refused")
	}
	if n := stateLogRows(t, tx, EntityInvoice, id); n != 0 {
		t.Fatalf("state_log has %d rows for a refused jump", n)
	}
}

func TestTransitionTask_TwoStepChainInOneTx(t *testing.T) {
	ctx := context.Background()
	r := NewRepo(testenv.OpenMySQL(t, "invoice"))
	invoiceID := seedInvoice(t, r)
	cmdID, err := r.InsertCommand(ctx, r.db, invoiceID, domain.KindIssue, "")
	if err != nil {
		t.Fatal(err)
	}
	taskID, err := r.InsertTask(ctx, r.db, cmdID, invoiceID, "air-a")
	if err != nil {
		t.Fatal(err)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ev := "44444444-4444-4444-4444-444444444444"
	applied, err := r.TransitionTask(ctx, tx, taskID, domain.TaskPending, domain.TaskRunning, 0, "provider.task.submitted", ev)
	if err != nil || !applied {
		t.Fatalf("pending -> running: applied=%v err=%v", applied, err)
	}
	applied, err = r.TransitionTask(ctx, tx, taskID, domain.TaskRunning, domain.TaskWaitingCallback, 1, "provider.task.submitted", ev)
	if err != nil || !applied {
		t.Fatalf("running -> waiting_callback at version 1: applied=%v err=%v", applied, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	task, err := r.GetTask(ctx, r.db, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != domain.TaskWaitingCallback || task.Version != 2 {
		t.Fatalf("task after chain: status=%s version=%d, want waiting_callback and 2", task.Status, task.Version)
	}
	if n := stateLogRows(t, r.db, EntityTask, taskID); n != 2 {
		t.Fatalf("state_log rows = %d, want 2", n)
	}
}
