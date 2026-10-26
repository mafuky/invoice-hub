//go:build lesson && integration

package app

import (
	"context"
	"database/sql"
	"github.com/mafuky/invoice-hub/internal/platform/consume"
	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
	"github.com/mafuky/invoice-hub/internal/platform/outbox"
	"sync"
	"testing"

	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
)

func TestApply_ConcurrentSameBizOrderNo_OneInvoice(t *testing.T) {
	forEachPersistence(t, func(t *testing.T, impl string) {
		ctx := context.Background()
		db := testenv.OpenMySQL(t, "invoice")
		svc := NewService(db, newRepo(t, db, impl), persistenceOption(impl))

		amt, err := money.Parse("1999.99")
		if err != nil {
			t.Fatal(err)
		}
		in := ApplyInput{
			BizOrderNo: testenv.BizOrderNo(), ProductLine: "FLIGHT", Title: "Example Travel Co., Ltd.",
			TaxNo: "91310000EXAMPLE001", Amount: amt, CustomerID: "CUST-0001",
		}

		const callers = 20
		type result struct {
			id      int64
			existed bool
			err     error
		}
		results := make([]result, callers)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				id, existed, err := svc.Apply(ctx, in)
				results[i] = result{id, existed, err}
			}()
		}
		close(start)
		wg.Wait()

		created := 0
		for i, r := range results {
			if r.err != nil {
				t.Fatalf("caller %d: %v", i, r.err)
			}
			if r.id != results[0].id {
				t.Errorf("caller %d got invoice %d, caller 0 got %d", i, r.id, results[0].id)
			}
			if !r.existed {
				created++
			}
		}
		if created != 1 {
			t.Errorf("%d callers saw existed=false, want exactly 1", created)
		}

		var invoices int
		if err := db.QueryRow(`SELECT COUNT(*) FROM invoices WHERE biz_order_no = ?`, in.BizOrderNo).Scan(&invoices); err != nil {
			t.Fatal(err)
		}
		if invoices != 1 {
			t.Errorf("invoices rows = %d, want 1", invoices)
		}
		var events int
		err = db.QueryRow(
			`SELECT COUNT(*) FROM outbox WHERE routing_key = 'invoice.task.created' AND JSON_EXTRACT(body, '$.payload.invoice_id') = ?`,
			results[0].id).Scan(&events)
		if err != nil {
			t.Fatal(err)
		}
		if events != 1 {
			t.Errorf("task created events = %d, want 1", events)
		}
		countRows(t, db, `SELECT COUNT(*) FROM commands WHERE invoice_id=?`, 1, results[0].id)
		countRows(t, db, `SELECT COUNT(*) FROM tasks WHERE invoice_id=?`, 1, results[0].id)

	})
}

func TestApply_PersistenceRouting(t *testing.T) {
	forEachPersistence(t, func(t *testing.T, impl string) {
		db := testenv.OpenMySQL(t, "invoice")
		inserted := 0
		var emitted event.TaskCreated
		insert := func(ctx context.Context, tx *sql.Tx, key string, env event.Envelope) error {
			inserted++
			if err := env.Decode(&emitted); err != nil {
				t.Fatal(err)
			}
			if tx == nil || key != mq.InvoiceTaskCreated {
				t.Fatal("wrong outbox routing arguments")
			}
			return outbox.ForImplementation(impl)(ctx, tx, key, env)
		}
		s := NewService(db, newRepo(t, db, impl), WithPersistence(consume.ForImplementation(impl), insert))
		id, existed, err := s.Apply(context.Background(), requestInput(t))
		if inserted != 1 {
			t.Fatalf("insert=%d err=%v", inserted, err)
		}
		if err != nil || existed || emitted.InvoiceID != id {
			t.Fatalf("id=%d existed=%v emitted=%+v err=%v", id, existed, emitted, err)
		}
		graphCounts(t, db, 1, 1, 1, 1)
	})
}

func TestApply_OutboxFailureRollsBackInvoiceCommandAndTask(t *testing.T) {
	forEachPersistence(t, func(t *testing.T, impl string) {
		db := testenv.OpenMySQL(t, "invoice")
		svc := NewService(db, newRepo(t, db, impl), persistenceOption(impl))
		restore := failOutbox(t, db)
		if _, _, err := svc.Apply(context.Background(), requestInput(t)); err == nil {
			t.Fatal("outbox failure accepted")
		}
		restore()
		graphCounts(t, db, 0, 0, 0, 0)

	})
}

func TestApply_TaskCreatedPayloadMatchesPersistedGraph(t *testing.T) {
	forEachPersistence(t, func(t *testing.T, impl string) {
		db := testenv.OpenMySQL(t, "invoice")
		r := newRepo(t, db, impl)
		svc := NewService(db, r, persistenceOption(impl))
		in := requestInput(t)
		id, existed, err := svc.Apply(context.Background(), in)
		if err != nil || existed {
			t.Fatalf("existed=%v err=%v", existed, err)
		}
		inv, err := r.GetInvoice(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		assertFields(t, inv, in)
		if inv.Status != "applying" {
			t.Fatalf("status=%s", inv.Status)
		}
		var cmd int64
		if err := db.QueryRow(`SELECT id FROM commands WHERE invoice_id=?`, id).Scan(&cmd); err != nil {
			t.Fatal(err)
		}
		assertPendingGraph(t, &invoiceFixture{db: db, repo: r}, id, cmd, "ISSUE", "")
		graphCounts(t, db, 1, 1, 1, 1)

	})
}

func TestApply_NewRequest_CreatesCommandPendingTaskAndEvent(t *testing.T) {
	forEachPersistence(t, func(t *testing.T, impl string) {
		ctx := context.Background()
		db := testenv.OpenMySQL(t, "invoice")
		svc := NewService(db, newRepo(t, db, impl), persistenceOption(impl))

		amt, _ := money.Parse("100.10")
		id, existed, err := svc.Apply(ctx, ApplyInput{
			BizOrderNo: testenv.BizOrderNo(), ProductLine: "HOTEL", Title: "Example Travel Co., Ltd.",
			TaxNo: "91310000EXAMPLE001", Amount: amt, CustomerID: "CUST-0002",
		})
		if err != nil || existed {
			t.Fatalf("existed=%v err=%v", existed, err)
		}

		var cmds, tasks int
		if err := db.QueryRow(`SELECT COUNT(*) FROM commands WHERE invoice_id = ? AND kind = 'ISSUE' AND status = 'created'`, id).Scan(&cmds); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM tasks WHERE invoice_id = ? AND status = 'pending'`, id).Scan(&tasks); err != nil {
			t.Fatal(err)
		}
		if cmds != 1 || tasks != 1 {
			t.Fatalf("commands=%d pending tasks=%d, want 1 and 1", cmds, tasks)
		}
		var status string
		if err := db.QueryRow(`SELECT status FROM invoices WHERE id = ?`, id).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "applying" {
			t.Fatalf("new invoice status = %s, want applying", status)
		}

	})
}
