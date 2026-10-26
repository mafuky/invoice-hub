//go:build lesson && integration

package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/invoice/store"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
)

func seedInvoice(t *testing.T, r *testRepo) int64 {
	t.Helper()
	amt, err := money.Parse("1999.99")
	if err != nil {
		t.Fatal(err)
	}
	id, err := r.InsertInvoice(context.Background(), r.db, store.NewInvoice{
		BizOrderNo: testenv.BizOrderNo(), ProductLine: "FLIGHT", Title: "Example Travel Co., Ltd.",
		TaxNo: "91310000EXAMPLE001", Amount: amt, CustomerID: "CUST-0001",
	})
	if err != nil {
		t.Fatalf("seed invoice: %v", err)
	}
	return id
}

func stateLogRows(t *testing.T, q store.DBTX, entity string, id int64) int {
	t.Helper()
	var n int
	err := q.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM state_log WHERE entity = ? AND entity_id = ?`, entity, id).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestTransitionInvoice_VersionMismatch(t *testing.T) {
	forEachStore(t, func(t *testing.T, r *testRepo) {
		ctx := context.Background()
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

		if n := stateLogRows(t, r.db, store.EntityInvoice, id); n != 0 {
			t.Fatalf("state_log has %d rows for a refused transition", n)
		}
		inv, err := r.GetInvoice(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if inv.Status != domain.InvoiceApplying || inv.Version != 0 {
			t.Fatalf("invoice changed: status=%s version=%d", inv.Status, inv.Version)
		}
	})
}

func TestTransitionInvoice_AppliedWritesOneStateLogRow(t *testing.T) {
	forEachStore(t, func(t *testing.T, r *testRepo) {
		ctx := context.Background()
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

		if n := stateLogRows(t, r.db, store.EntityInvoice, id); n != 1 {
			t.Fatalf("state_log rows = %d, want 1", n)
		}
		inv, err := r.GetInvoice(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if inv.Status != domain.InvoiceIssuing || inv.Version != 1 {
			t.Fatalf("invoice after move: status=%s version=%d", inv.Status, inv.Version)
		}
	})
}

func TestTransitionInvoice_IllegalJump_Refused(t *testing.T) {
	forEachStore(t, func(t *testing.T, r *testRepo) {
		ctx := context.Background()
		id := seedInvoice(t, r)

		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		// delivery.invoice.delivered arriving before provider.task.succeeded
		applied, err := r.TransitionInvoice(ctx, tx, id, domain.InvoiceApplying, domain.InvoiceDelivered, 0, "delivery.invoice.delivered", "33333333-3333-3333-3333-333333333333")
		if errors.Is(err, errors.ErrUnsupported) {
			t.Fatalf("transition is still a lesson stub: %v", err)
		}
		if applied {
			t.Fatal("applying -> delivered must be refused")
		}
		if n := stateLogRows(t, tx, store.EntityInvoice, id); n != 0 {
			t.Fatalf("state_log has %d rows for a refused jump", n)
		}
		inv, err := r.GetInvoiceTx(ctx, tx, id)
		if err != nil {
			t.Fatal(err)
		}
		if inv.Status != domain.InvoiceApplying || inv.Version != 0 {
			t.Fatalf("refused jump changed invoice: %+v", inv)
		}
	})
}

func taskRefusal(t *testing.T, r *testRepo, current, from, to domain.TaskStatus, version int, illegal bool) {
	t.Helper()
	ctx := context.Background()
	invoiceID := seedInvoice(t, r)
	cmd, err := r.InsertCommand(ctx, r.db, invoiceID, domain.KindIssue, "")
	if err != nil {
		t.Fatal(err)
	}
	id, err := r.InsertTask(ctx, r.db, cmd, invoiceID, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.db.Exec(`UPDATE tasks SET status=? WHERE id=?`, current, id); err != nil {
		t.Fatal(err)
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ok, err := r.TransitionTask(ctx, tx, id, from, to, version, "provider.task.succeeded", "")
	if errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("transition is still a lesson stub: %v", err)
	}
	if ok {
		t.Fatal("refused task transition applied")
	}
	if !illegal && err != nil {
		t.Fatal(err)
	}
	task, err := r.GetTask(ctx, tx, id)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != current || task.Version != 0 {
		t.Fatalf("changed task: %+v", task)
	}
	if n := stateLogRows(t, tx, store.EntityTask, id); n != 0 {
		t.Fatalf("refused transition logs=%d", n)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestTransitionTask_StaleVersionLeavesStateAndLogUnchanged(t *testing.T) {
	forEachStore(t, func(t *testing.T, r *testRepo) {
		taskRefusal(t, r, domain.TaskPending, domain.TaskPending, domain.TaskRunning, 7, false)
	})
}
func TestTransitionTask_WrongFromLeavesStateAndLogUnchanged(t *testing.T) {
	forEachStore(t, func(t *testing.T, r *testRepo) {
		taskRefusal(t, r, domain.TaskPending, domain.TaskRunning, domain.TaskWaitingCallback, 0, false)
	})
}
func TestTransitionTask_TerminalAndIllegalMovesRefused(t *testing.T) {
	forEachStore(t, func(t *testing.T, r *testRepo) {
		for _, tc := range []struct{ from, to domain.TaskStatus }{{domain.TaskDone, domain.TaskFailed}, {domain.TaskFailed, domain.TaskDone}, {domain.TaskPending, domain.TaskDone}} {
			t.Run(string(tc.from)+" to "+string(tc.to), func(t *testing.T) { taskRefusal(t, r, tc.from, tc.from, tc.to, 0, true) })
		}
	})
}

func TestTransitions_CallerRollbackRevertsStateAndLog(t *testing.T) {
	forEachStore(t, func(t *testing.T, r *testRepo) {
		ctx := context.Background()
		id := seedInvoice(t, r)
		cmd, err := r.InsertCommand(ctx, r.db, id, domain.KindIssue, "")
		if err != nil {
			t.Fatal(err)
		}
		taskID, err := r.InsertTask(ctx, r.db, cmd, id, "")
		if err != nil {
			t.Fatal(err)
		}
		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if ok, err := r.TransitionInvoice(ctx, tx, id, domain.InvoiceApplying, domain.InvoiceIssuing, 0, "provider.task.submitted", ""); err != nil || !ok {
			t.Fatalf("invoice: %v %v", ok, err)
		}
		if ok, err := r.TransitionTask(ctx, tx, taskID, domain.TaskPending, domain.TaskRunning, 0, "provider.task.submitted", ""); err != nil || !ok {
			t.Fatalf("task: %v %v", ok, err)
		}
		for _, x := range []struct {
			entity string
			id     int64
		}{{store.EntityInvoice, id}, {store.EntityTask, taskID}} {
			if n := stateLogRows(t, tx, x.entity, x.id); n != 1 {
				t.Fatalf("logs before rollback=%d", n)
			}
		}
		inv, err := r.GetInvoiceTx(ctx, tx, id)
		if err != nil {
			t.Fatal(err)
		}
		task, err := r.GetTask(ctx, tx, taskID)
		if err != nil {
			t.Fatal(err)
		}
		if inv.Status != domain.InvoiceIssuing || inv.Version != 1 || task.Status != domain.TaskRunning || task.Version != 1 {
			t.Fatal("transitions missing inside tx")
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		inv, err = r.GetInvoice(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		task, err = r.GetTask(ctx, r.db, taskID)
		if err != nil {
			t.Fatal(err)
		}
		if inv.Status != domain.InvoiceApplying || inv.Version != 0 || task.Status != domain.TaskPending || task.Version != 0 {
			t.Fatalf("survived rollback: %+v %+v", inv, task)
		}
		for _, x := range []struct {
			entity string
			id     int64
		}{{store.EntityInvoice, id}, {store.EntityTask, taskID}} {
			if n := stateLogRows(t, r.db, x.entity, x.id); n != 0 {
				t.Fatalf("logs after rollback=%d", n)
			}
		}
	})
}

func TestTransitionTask_TwoStepChainInOneTx(t *testing.T) {
	forEachStore(t, func(t *testing.T, r *testRepo) {
		ctx := context.Background()
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
		if n := stateLogRows(t, r.db, store.EntityTask, taskID); n != 2 {
			t.Fatalf("state_log rows = %d, want 2", n)
		}
	})
}
