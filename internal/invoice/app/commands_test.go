//go:build lesson && integration

package app

import (
	"context"
	"testing"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/invoice/store"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
)

func TestReopen_CreatesOnePendingTaskAndOutbox(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "invoice")
	repo := store.NewRepo(db)
	svc := NewService(db, repo)

	amt, _ := money.Parse("1999.99")
	invoiceID, err := repo.InsertInvoice(ctx, db, store.NewInvoice{
		BizOrderNo: testenv.BizOrderNo(), ProductLine: "FLIGHT", Title: "Example Travel Co., Ltd.",
		TaxNo: "91310000EXAMPLE001", Amount: amt, CustomerID: "CUST-0001",
	})
	if err != nil {
		t.Fatal(err)
	}
	cmdID, err := repo.InsertCommand(ctx, db, invoiceID, domain.KindIssue, "")
	if err != nil {
		t.Fatal(err)
	}
	failedID, err := repo.InsertTask(ctx, db, cmdID, invoiceID, "air-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE tasks SET status = 'failed' WHERE id = ?`, failedID); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	newID, err := svc.Reopen(ctx, tx, invoiceID, cmdID, failedID)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var live int
	var status string
	var supplier *string
	if err := db.QueryRow(`SELECT COUNT(*) FROM tasks WHERE command_id = ? AND status NOT IN ('done','failed')`, cmdID).Scan(&live); err != nil {
		t.Fatal(err)
	}
	if live != 1 {
		t.Fatalf("live tasks for the command = %d, want 1", live)
	}
	if err := db.QueryRow(`SELECT status, supplier FROM tasks WHERE id = ?`, newID).Scan(&status, &supplier); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || supplier != nil {
		t.Fatalf("replacement task: status=%s supplier=%v, want pending and none", status, supplier)
	}
	var events int
	err = db.QueryRow(`SELECT COUNT(*) FROM outbox WHERE routing_key = 'invoice.task.created' AND JSON_EXTRACT(body, '$.payload.task_id') = ?`, newID).Scan(&events)
	if err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Fatalf("task created events for the replacement = %d, want 1", events)
	}
}
