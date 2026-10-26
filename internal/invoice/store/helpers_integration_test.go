//go:build integration

package store_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/invoice/store"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
)

func TestHelpers_InvoiceAndTaskRoundTrip(t *testing.T) {
	forEachStore(t, func(t *testing.T, r *testRepo) {
		ctx := context.Background()
		amount, err := money.Parse("100.10")
		if err != nil {
			t.Fatal(err)
		}
		in := store.NewInvoice{BizOrderNo: testenv.BizOrderNo(), ProductLine: "FLIGHT", Title: "Example Travel Co., Ltd.", TaxNo: "91310000EXAMPLE001", Amount: amount, CustomerID: "CUST-0001"}
		id, err := r.InsertInvoice(ctx, r.db, in)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.InsertInvoice(ctx, r.db, in); !store.IsDuplicateKey(err) {
			t.Fatalf("duplicate: %v", err)
		}
		inv, err := r.GetInvoiceByBizOrderNo(ctx, r.db, in.BizOrderNo)
		if err != nil {
			t.Fatal(err)
		}
		if inv.ID != id || inv.Amount.String() != "100.10" || inv.Status != domain.InvoiceApplying || inv.Version != 0 || inv.InvoiceNo != "" || inv.CreatedAt.IsZero() || inv.UpdatedAt.IsZero() || inv.Title != in.Title || inv.TaxNo != in.TaxNo || inv.ProductLine != in.ProductLine || inv.CustomerID != in.CustomerID {
			t.Fatalf("invoice: %+v", inv)
		}
		if err := r.SetInvoiceNo(ctx, r.db, id, "AIR-A-EXAMPLE001"); err != nil {
			t.Fatal(err)
		}
		rows, err := r.ListInvoices(ctx, "applying", 0, 1)
		if err != nil || len(rows) != 1 || rows[0].InvoiceNo != "AIR-A-EXAMPLE001" {
			t.Fatalf("list: %+v %v", rows, err)
		}
		for _, tc := range []struct {
			status string
			after  int64
			limit  int
		}{{"issued", 0, 10}, {"", id, 10}, {"", 0, 0}} {
			rows, err := r.ListInvoices(ctx, tc.status, tc.after, tc.limit)
			if err != nil || len(rows) != 0 {
				t.Fatalf("empty list: %+v %v", rows, err)
			}
		}
		cmd, err := r.InsertCommand(ctx, r.db, id, domain.KindIssue, "travel invoice")
		if err != nil {
			t.Fatal(err)
		}
		command, err := r.GetCommand(ctx, r.db, cmd)
		if err != nil || command.InvoiceID != id || command.Kind != domain.KindIssue || command.Reason != "travel invoice" || command.Status != domain.CommandCreated || command.Version != 0 {
			t.Fatalf("command: %+v %v", command, err)
		}
		taskID, err := r.InsertTask(ctx, r.db, cmd, id, "")
		if err != nil {
			t.Fatal(err)
		}
		task, err := r.GetTask(ctx, r.db, taskID)
		if err != nil || task.CommandID != cmd || task.InvoiceID != id || task.Supplier != "" || task.DeadlineAt != nil || task.Status != domain.TaskPending || task.Version != 0 || task.Attempt != 0 {
			t.Fatalf("task: %+v %v", task, err)
		}
		var supplier sql.NullString
		if err := r.db.QueryRow("SELECT supplier FROM tasks WHERE id=?", taskID).Scan(&supplier); err != nil || supplier.Valid {
			t.Fatalf("supplier must be NULL: %+v %v", supplier, err)
		}
		at := time.Date(2026, 10, 26, 12, 0, 0, 123000000, time.FixedZone("test", 8*3600))
		if err := r.SetTaskDeadline(ctx, r.db, taskID, at); err != nil {
			t.Fatal(err)
		}
		if _, err := r.db.Exec("UPDATE tasks SET status=? WHERE id=?", domain.TaskWaitingCallback, taskID); err != nil {
			t.Fatal(err)
		}
		tasks, err := r.ListTimedOutTasks(ctx, at, 1)
		if err != nil || len(tasks) != 1 || tasks[0].ID != taskID || tasks[0].DeadlineAt == nil || !tasks[0].DeadlineAt.Equal(at) {
			t.Fatalf("timed out: %+v %v", tasks, err)
		}
		for _, tc := range []struct {
			at    time.Time
			limit int
		}{{at.Add(-time.Millisecond), 1}, {at, 0}} {
			tasks, err := r.ListTimedOutTasks(ctx, tc.at, tc.limit)
			if err != nil || len(tasks) != 0 {
				t.Fatalf("not timed out: %+v %v", tasks, err)
			}
		}
		if err := r.AppendStateLog(ctx, r.db, store.EntityTask, taskID, "failed", "failed", "late_outcome", ""); err != nil {
			t.Fatal(err)
		}
		var eventID sql.NullString
		var loggedAt time.Time
		if err := r.db.QueryRow("SELECT event_id, at FROM state_log WHERE entity_id=?", taskID).Scan(&eventID, &loggedAt); err != nil || eventID.Valid || loggedAt.IsZero() {
			t.Fatalf("state log defaults: %+v %v %v", eventID, loggedAt, err)
		}
		_, invoiceErr := r.GetInvoice(ctx, -1)
		_, bizErr := r.GetInvoiceByBizOrderNo(ctx, r.db, "BIZ-20261026-9999-missing")
		_, taskErr := r.GetTask(ctx, r.db, -1)
		_, commandErr := r.GetCommand(ctx, r.db, -1)
		for _, err := range []error{invoiceErr, bizErr, taskErr, commandErr, r.SetInvoiceNo(ctx, r.db, -1, "missing")} {
			if !errors.Is(err, domain.ErrNotFound) {
				t.Fatalf("not found: %v", err)
			}
		}
		if err := r.SetTaskDeadline(ctx, r.db, -1, at); err != nil {
			t.Fatalf("missing deadline update: %v", err)
		}
	})
}

func TestHelpers_CallerRollbackRevertsInserts(t *testing.T) {
	forEachStore(t, func(t *testing.T, r *testRepo) {
		ctx := context.Background()
		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		amount, _ := money.Parse("1999.99")
		id, err := r.InsertInvoice(ctx, tx, store.NewInvoice{BizOrderNo: testenv.BizOrderNo(), ProductLine: "TRAIN", Title: "Example Travel Co., Ltd.", TaxNo: "91310000EXAMPLE001", Amount: amount, CustomerID: "CUST-0001"})
		if err != nil {
			t.Fatal(err)
		}
		cmd, err := r.InsertCommand(ctx, tx, id, domain.KindIssue, "")
		if err != nil {
			t.Fatal(err)
		}
		taskID, err := r.InsertTask(ctx, tx, cmd, id, "rail-a")
		if err != nil {
			t.Fatal(err)
		}
		task, err := r.GetTask(ctx, tx, taskID)
		if err != nil || task.Supplier != "rail-a" {
			t.Fatalf("task in tx: %+v %v", task, err)
		}
		if err := r.AppendStateLog(ctx, tx, store.EntityInvoice, id, "", "applying", "issue request", ""); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		for _, table := range []string{"invoices", "commands", "tasks", "state_log"} {
			var count int
			if err := r.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
				t.Fatalf("%s survived rollback: %d %v", table, count, err)
			}
		}
	})
}

func TestHelpers_ForUpdateHoldsRowsUntilRollback(t *testing.T) {
	forEachStore(t, func(t *testing.T, r *testRepo) {
		ctx := context.Background()
		amount, _ := money.Parse("100.10")
		id, err := r.InsertInvoice(ctx, r.db, store.NewInvoice{BizOrderNo: testenv.BizOrderNo(), ProductLine: "HOTEL", Title: "Example Travel Co., Ltd.", TaxNo: "91310000EXAMPLE001", Amount: amount, CustomerID: "CUST-0001"})
		if err != nil {
			t.Fatal(err)
		}
		cmd, err := r.InsertCommand(ctx, r.db, id, domain.KindIssue, "")
		if err != nil {
			t.Fatal(err)
		}
		taskID, err := r.InsertTask(ctx, r.db, cmd, id, "hotel-a")
		if err != nil {
			t.Fatal(err)
		}
		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err := r.GetInvoiceForUpdate(ctx, tx, id); err != nil {
			t.Fatal(err)
		}
		if _, err := r.GetCommandForUpdate(ctx, tx, cmd); err != nil {
			t.Fatal(err)
		}
		if _, err := r.GetTaskForUpdate(ctx, tx, taskID); err != nil {
			t.Fatal(err)
		}
		other, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer other.Rollback()
		for _, row := range []struct {
			table string
			id    int64
		}{{"invoices", id}, {"commands", cmd}, {"tasks", taskID}} {
			var got int64
			err := other.QueryRowContext(ctx, "SELECT id FROM "+row.table+" WHERE id=? FOR UPDATE NOWAIT", row.id).Scan(&got)
			var me *mysql.MySQLError
			if !errors.As(err, &me) || me.Number != 3572 {
				t.Fatalf("%s was not locked: %v", row.table, err)
			}
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if _, err := r.GetInvoiceForUpdate(ctx, other, id); err != nil {
			t.Fatal(err)
		}
		if _, err := r.GetCommandForUpdate(ctx, other, cmd); err != nil {
			t.Fatal(err)
		}
		if _, err := r.GetTaskForUpdate(ctx, other, taskID); err != nil {
			t.Fatal(err)
		}
	})
}
