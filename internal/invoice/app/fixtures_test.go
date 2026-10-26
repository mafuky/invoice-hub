//go:build lesson && integration

package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/invoice/store"
	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
)

type invoiceFixture struct {
	db        *sql.DB
	repo      *store.Repo
	svc       *Service
	events    *Events
	inv       domain.Invoice
	cmd, task int64
}

func requestInput(t *testing.T) ApplyInput {
	t.Helper()
	amount, err := money.Parse("100.10")
	if err != nil {
		t.Fatal(err)
	}
	return ApplyInput{BizOrderNo: testenv.BizOrderNo(), ProductLine: "HOTEL", Title: "Example Travel Co., Ltd.", TaxNo: "91310000EXAMPLE001", Amount: amount, CustomerID: "CUST-0002"}
}

func seedFixture(t *testing.T, status domain.InvoiceStatus, kind string, taskStatus domain.TaskStatus) *invoiceFixture {
	t.Helper()
	db := testenv.OpenMySQL(t, "invoice")
	r := store.NewRepo(db)
	f := &invoiceFixture{db: db, repo: r, svc: NewService(db, r), events: NewEvents(db, r, time.Minute)}
	in := requestInput(t)
	id, err := r.InsertInvoice(context.Background(), db, store.NewInvoice(in))
	if err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, `UPDATE invoices SET status=?, invoice_no='HOTEL-OLD-0001' WHERE id=?`, status, id)
	f.inv, err = r.GetInvoice(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if kind != "" {
		f.cmd, err = r.InsertCommand(context.Background(), db, id, kind, "ticket refunded")
		if err != nil {
			t.Fatal(err)
		}
		f.task, err = r.InsertTask(context.Background(), db, f.cmd, id, "air-a")
		if err != nil {
			t.Fatal(err)
		}
		execSQL(t, db, `UPDATE tasks SET status=? WHERE id=?`, taskStatus, f.task)
		if taskStatus != domain.TaskPending {
			execSQL(t, db, `UPDATE commands SET status='dispatched', version=1 WHERE id=?`, f.cmd)
		}
	}
	return f
}

func execSQL(t *testing.T, q store.DBTX, query string, args ...any) {
	t.Helper()
	if _, err := q.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatal(err)
	}
}

func countRows(t *testing.T, q store.DBTX, query string, want int, args ...any) {
	t.Helper()
	var n int
	if err := q.QueryRowContext(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != want {
		t.Fatalf("%s: count=%d want=%d", query, n, want)
	}
}

func graphCounts(t *testing.T, q store.DBTX, invoices, commands, tasks, events int) {
	t.Helper()
	for _, x := range []struct {
		table string
		n     int
	}{{"invoices", invoices}, {"commands", commands}, {"tasks", tasks}, {"outbox", events}} {
		countRows(t, q, "SELECT COUNT(*) FROM "+x.table, x.n)
	}
}

func beginTx(t *testing.T, db *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}

func commitTx(t *testing.T, tx *sql.Tx) {
	t.Helper()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func assertInvoiceUnchanged(t *testing.T, f *invoiceFixture) {
	t.Helper()
	got, err := f.repo.GetInvoice(context.Background(), f.inv.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, f.inv) {
		t.Fatalf("invoice changed: %+v -> %+v", f.inv, got)
	}
}

func assertFields(t *testing.T, got domain.Invoice, in ApplyInput) {
	t.Helper()
	if got.BizOrderNo != in.BizOrderNo || got.ProductLine != in.ProductLine || got.Title != in.Title || got.TaxNo != in.TaxNo || got.Amount.String() != in.Amount.String() || got.CustomerID != in.CustomerID {
		t.Fatalf("invoice fields: %+v want %+v", got, in)
	}
}

func assertTaskEvent(t *testing.T, q store.DBTX, inv domain.Invoice, taskID int64, kind string) {
	t.Helper()
	countRows(t, q, `SELECT COUNT(*) FROM outbox WHERE routing_key=? AND JSON_EXTRACT(body,'$.payload.task_id')=?`, 1, mq.InvoiceTaskCreated, taskID)
	var body []byte
	var key string
	if err := q.QueryRowContext(context.Background(), `SELECT routing_key,body FROM outbox WHERE JSON_EXTRACT(body,'$.payload.task_id')=?`, taskID).Scan(&key, &body); err != nil {
		t.Fatal(err)
	}
	var env event.Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatal(err)
	}
	var p event.TaskCreated
	if err := env.Decode(&p); err != nil {
		t.Fatal(err)
	}
	if key != mq.InvoiceTaskCreated || env.Type != key || env.EventID == "" || env.AggregateID != strconv.FormatInt(inv.ID, 10) || env.Version != inv.Version {
		t.Fatalf("bad envelope: %+v", env)
	}
	if p.TaskID != taskID || p.InvoiceID != inv.ID || p.CommandKind != kind || p.ProductLine != inv.ProductLine || p.Title != inv.Title || p.TaxNo != inv.TaxNo || p.Amount.String() != inv.Amount.String() || p.CustomerID != inv.CustomerID {
		t.Fatalf("bad task payload: %+v for %+v", p, inv)
	}
}

func assertPendingGraph(t *testing.T, f *invoiceFixture, invoiceID, commandID int64, kind, reason string) int64 {
	t.Helper()
	ctx := context.Background()
	cmd, err := f.repo.GetCommand(ctx, f.db, commandID)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.InvoiceID != invoiceID || cmd.Kind != kind || (reason != "" && cmd.Reason != reason) || cmd.Status != domain.CommandCreated {
		t.Fatalf("bad command: %+v", cmd)
	}
	countRows(t, f.db, `SELECT COUNT(*) FROM tasks WHERE command_id=?`, 1, commandID)
	var taskID int64
	if err := f.db.QueryRow(`SELECT id FROM tasks WHERE command_id=?`, commandID).Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	task, err := f.repo.GetTask(ctx, f.db, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if task.InvoiceID != invoiceID || task.CommandID != commandID || task.Status != domain.TaskPending {
		t.Fatalf("bad task: %+v", task)
	}
	inv, err := f.repo.GetInvoice(ctx, invoiceID)
	if err != nil {
		t.Fatal(err)
	}
	assertTaskEvent(t, f.db, inv, taskID, kind)
	return taskID
}

func sendEvent(t *testing.T, f *invoiceFixture, key string, taskID int64) event.Envelope {
	t.Helper()
	var payload any = event.TaskOutcome{TaskID: taskID, InvoiceID: f.inv.ID, Supplier: "air-a", SupplierOrderNo: "AIR-A-ORD-0001", InvoiceNo: "AIR-A-12345678", Reason: "supplier outcome"}
	if key == mq.ProviderTaskSubmitted {
		payload = event.TaskSubmitted{TaskID: taskID, InvoiceID: f.inv.ID, Supplier: "air-a", SupplierOrderNo: "AIR-A-ORD-0001"}
	}
	env, err := event.New(key, strconv.FormatInt(f.inv.ID, 10), 1, payload, "trace-lifecycle")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.events.Handle(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	return env
}

func failOutbox(t *testing.T, db *sql.DB) func() {
	t.Helper()
	execSQL(t, db, `RENAME TABLE outbox TO outbox_failure_saved`)
	restored := false
	remove := func() {
		if !restored {
			execSQL(t, db, `RENAME TABLE outbox_failure_saved TO outbox`)
			restored = true
		}
	}
	t.Cleanup(remove)
	return remove
}
