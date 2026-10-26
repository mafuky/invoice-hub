//go:build integration

package app

import (
	"context"
	"testing"
	"time"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/invoice/store"
	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
)

func TestEvents_CommandLifecycle_TerminalOutcomesDoNotMoveCommands(t *testing.T) {
	for _, taskStatus := range []domain.TaskStatus{domain.TaskDone, domain.TaskFailed} {
		t.Run(string(taskStatus), func(t *testing.T) {
			ctx := context.Background()
			db := testenv.OpenMySQL(t, "invoice")
			r := store.NewRepo(db)
			amount, err := money.Parse("100.10")
			if err != nil {
				t.Fatal(err)
			}
			id, err := r.InsertInvoice(ctx, db, store.NewInvoice{BizOrderNo: testenv.BizOrderNo(), ProductLine: "HOTEL", Title: "Example Travel Co., Ltd.", TaxNo: "91310000EXAMPLE001", Amount: amount, CustomerID: "CUST-0002"})
			if err != nil {
				t.Fatal(err)
			}
			cmd, err := r.InsertCommand(ctx, db, id, domain.KindRedFlush, "ticket refunded")
			if err != nil {
				t.Fatal(err)
			}
			task, err := r.InsertTask(ctx, db, cmd, id, "air-a")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE tasks SET status=? WHERE id=?`, taskStatus, task); err != nil {
				t.Fatal(err)
			}
			status := domain.CommandSucceeded
			if taskStatus == domain.TaskFailed {
				status = domain.CommandFailed
			}
			if _, err := db.Exec(`UPDATE commands SET status=?,version=2 WHERE id=?`, status, cmd); err != nil {
				t.Fatal(err)
			}
			e := NewEvents(db, r, time.Minute)
			for _, key := range []string{mq.ProviderTaskSucceeded, mq.ProviderTaskFailed} {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				err = e.ApplyOutcome(ctx, tx, key, event.TaskOutcome{TaskID: task, InvoiceID: id, InvoiceNo: "AIR-A-12345678"}, "11111111-1111-1111-1111-111111111111", "")
				if err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			c, err := r.GetCommand(ctx, db, cmd)
			if err != nil {
				t.Fatal(err)
			}
			if c.Status != status || c.Version != 2 {
				t.Fatalf("terminal command moved: %+v", c)
			}
			var n int
			if err := db.QueryRow(`SELECT COUNT(*) FROM state_log WHERE entity='command'`).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Fatalf("command logs=%d", n)
			}
		})
	}
}
