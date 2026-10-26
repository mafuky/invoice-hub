//go:build integration

package app

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/invoice/store"
	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
	"github.com/mafuky/invoice-hub/internal/platform/outbox"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
)

func TestEvents_EmitUsesInjectedInserter(t *testing.T) {
	forEachPersistence(t, func(t *testing.T, impl string) {
		db := testenv.OpenMySQL(t, "invoice")
		r := newRepo(t, db, impl)
		ctx := context.Background()
		amount, _ := money.Parse("100.10")
		id, err := r.InsertInvoice(ctx, db, store.NewInvoice{BizOrderNo: testenv.BizOrderNo(), ProductLine: "TRAIN", Title: "Example Travel Co., Ltd.", TaxNo: "91310000EXAMPLE001", Amount: amount, CustomerID: "CUST-0001"})
		if err != nil {
			t.Fatal(err)
		}
		if err := r.SetInvoiceNo(ctx, db, id, "RAIL-A-12345678"); err != nil {
			t.Fatal(err)
		}
		calls := 0
		insert := func(ctx context.Context, tx *sql.Tx, key string, env event.Envelope) error {
			calls++
			var issued event.InvoiceIssued
			if err := env.Decode(&issued); err != nil {
				t.Fatal(err)
			}
			if key != mq.InvoiceIssued || issued.InvoiceID != id || issued.InvoiceNo != "RAIL-A-12345678" {
				t.Fatalf("key=%s payload=%+v", key, issued)
			}
			return outbox.ForImplementation(impl)(ctx, tx, key, env)
		}
		e := NewEvents(db, r, time.Minute, WithPersistence(nil, insert))
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if err := e.emit(ctx, tx, Plan{InvoiceID: id, Emit: mq.InvoiceIssued}, "trace-routing"); err != nil {
			t.Fatal(err)
		}
		if calls != 1 {
			t.Fatalf("insert calls=%d", calls)
		}
		var n int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM outbox WHERE routing_key=?`, mq.InvoiceIssued).Scan(&n); err != nil || n != 1 {
			t.Fatalf("outbox=%d err=%v", n, err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM outbox`).Scan(&n); err != nil || n != 0 {
			t.Fatalf("outbox survived rollback=%d err=%v", n, err)
		}
	})
}

func TestEvents_CommandLifecycle_TerminalOutcomesDoNotMoveCommands(t *testing.T) {
	forEachPersistence(t, func(t *testing.T, impl string) {
		for _, taskStatus := range []domain.TaskStatus{domain.TaskDone, domain.TaskFailed} {
			t.Run(string(taskStatus), func(t *testing.T) {
				ctx := context.Background()
				db := testenv.OpenMySQL(t, "invoice")
				r := newRepo(t, db, impl)
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
				e := NewEvents(db, r, time.Minute, persistenceOption(impl))
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

	})
}
