//go:build lesson && integration

package app

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"

	providerv1 "github.com/mafuky/invoice-hub/gen/invoicehub/provider/v1"
	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/invoice/store"
	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
)

// noSuchOrder is a provider-svc whose suppliers know no order.
type noSuchOrder struct{ queried []int64 }

func (f *noSuchOrder) HandleCallback(context.Context, *providerv1.CallbackRequest, ...grpc.CallOption) (*providerv1.CallbackReply, error) {
	return &providerv1.CallbackReply{}, nil
}

func (f *noSuchOrder) QueryOrder(_ context.Context, in *providerv1.QueryOrderRequest, _ ...grpc.CallOption) (*providerv1.QueryOrderReply, error) {
	f.queried = append(f.queried, in.GetTaskId())
	return &providerv1.QueryOrderReply{Found: false}, nil
}

func TestScanner_TimeoutQueriesThenReopens(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "invoice")
	repo := store.NewRepo(db)

	amt, _ := money.Parse("1999.99")
	invoiceID, err := repo.InsertInvoice(ctx, db, store.NewInvoice{
		BizOrderNo: testenv.BizOrderNo(), ProductLine: "FLIGHT", Title: "Example Travel Co., Ltd.",
		TaxNo: "91310000EXAMPLE001", Amount: amt, CustomerID: "CUST-0001",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE invoices SET status = 'issuing' WHERE id = ?`, invoiceID); err != nil {
		t.Fatal(err)
	}
	cmdID, err := repo.InsertCommand(ctx, db, invoiceID, domain.KindIssue, "")
	if err != nil {
		t.Fatal(err)
	}
	taskID, err := repo.InsertTask(ctx, db, cmdID, invoiceID, "air-a")
	if err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, `UPDATE commands SET status='dispatched', version=1 WHERE id=?`, cmdID)
	_, err = db.Exec(`UPDATE tasks SET status = 'waiting_callback', deadline_at = ? WHERE id = ?`,
		time.Now().UTC().Add(-time.Minute), taskID)
	if err != nil {
		t.Fatal(err)
	}

	provider := &noSuchOrder{}
	sc := NewScanner(db, repo, provider, NewEvents(db, repo, 2*time.Minute))
	handled, err := sc.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if handled < 1 {
		t.Fatalf("handled = %d, want at least 1", handled)
	}

	asked := false
	for _, id := range provider.queried {
		asked = asked || id == taskID
	}
	if !asked {
		t.Error("the supplier was not queried before the task was given up")
	}

	var status string
	if err := db.QueryRow(`SELECT status FROM tasks WHERE id = ?`, taskID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "failed" {
		t.Errorf("timed-out task status = %s, want failed", status)
	}

	var newID int64
	var newStatus string
	var supplier *string
	err = db.QueryRow(`SELECT id, status, supplier FROM tasks WHERE command_id = ? AND id <> ?`, cmdID, taskID).Scan(&newID, &newStatus, &supplier)
	if err != nil {
		t.Fatalf("no replacement task for the command: %v", err)
	}
	if newStatus != "pending" || supplier != nil {
		t.Errorf("replacement task: status=%s supplier=%v, want pending and no supplier", newStatus, supplier)
	}

	var events int
	err = db.QueryRow(
		`SELECT COUNT(*) FROM outbox WHERE routing_key = 'invoice.task.created' AND JSON_EXTRACT(body, '$.payload.task_id') = ?`,
		newID).Scan(&events)
	if err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Errorf("task created events for the replacement task = %d, want 1", events)
	}
}

type queryProvider struct {
	providerv1.ProviderServiceClient
	query func(context.Context, *providerv1.QueryOrderRequest) (*providerv1.QueryOrderReply, error)
}

func (p queryProvider) QueryOrder(ctx context.Context, in *providerv1.QueryOrderRequest, _ ...grpc.CallOption) (*providerv1.QueryOrderReply, error) {
	return p.query(ctx, in)
}

func timeoutFixture(t *testing.T, query func(context.Context, *providerv1.QueryOrderRequest) (*providerv1.QueryOrderReply, error)) (*invoiceFixture, *Scanner) {
	t.Helper()
	f := seedFixture(t, domain.InvoiceIssuing, domain.KindIssue, domain.TaskWaitingCallback)
	execSQL(t, f.db, `UPDATE tasks SET deadline_at=? WHERE id=?`, time.Now().UTC().Add(-time.Minute), f.task)
	return f, NewScanner(f.db, f.repo, queryProvider{query: query}, f.events)
}

func assertIssuedOutcome(t *testing.T, f *invoiceFixture) {
	t.Helper()
	inv, err := f.repo.GetInvoice(context.Background(), f.inv.ID)
	if err != nil {
		t.Fatal(err)
	}
	if inv.Status != domain.InvoiceIssued || inv.InvoiceNo != "AIR-A-12345678" || inv.Version != f.inv.Version+1 {
		t.Fatalf("invoice=%+v", inv)
	}
	countRows(t, f.db, `SELECT COUNT(*) FROM tasks WHERE id=? AND status='done' AND version=1`, 1, f.task)
	graphCounts(t, f.db, 1, 1, 1, 1)
	var body []byte
	var key string
	if err := f.db.QueryRow(`SELECT routing_key,body FROM outbox`).Scan(&key, &body); err != nil {
		t.Fatal(err)
	}
	var env event.Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatal(err)
	}
	var p event.InvoiceIssued
	if err := env.Decode(&p); err != nil {
		t.Fatal(err)
	}
	if key != mq.InvoiceIssued || env.Type != key || env.Version != inv.Version || p.InvoiceID != inv.ID || p.Version != inv.Version || p.InvoiceNo != inv.InvoiceNo || p.CustomerID != inv.CustomerID || p.ProductLine != inv.ProductLine || p.Title != inv.Title || p.Amount.String() != inv.Amount.String() {
		t.Fatalf("issued envelope=%+v payload=%+v", env, p)
	}
}

func TestScanner_QueryFoundSucceededAppliesOutcomeWithoutReopen(t *testing.T) {
	f, sc := timeoutFixture(t, func(context.Context, *providerv1.QueryOrderRequest) (*providerv1.QueryOrderReply, error) {
		return &providerv1.QueryOrderReply{Found: true, Outcome: providerv1.OrderOutcome_ORDER_OUTCOME_SUCCEEDED, InvoiceNo: "AIR-A-12345678"}, nil
	})
	if _, err := sc.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertIssuedOutcome(t, f)
}

func TestScanner_QueryFoundFailedReopensOnce(t *testing.T) {
	f, sc := timeoutFixture(t, func(context.Context, *providerv1.QueryOrderRequest) (*providerv1.QueryOrderReply, error) {
		return &providerv1.QueryOrderReply{Found: true, Outcome: providerv1.OrderOutcome_ORDER_OUTCOME_FAILED}, nil
	})
	for range 2 {
		if _, err := sc.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	countRows(t, f.db, `SELECT COUNT(*) FROM tasks WHERE id=? AND status='failed' AND version=1`, 1, f.task)
	var next int64
	if err := f.db.QueryRow(`SELECT id FROM tasks WHERE command_id=? AND status='pending' AND supplier IS NULL`, f.cmd).Scan(&next); err != nil {
		t.Fatal(err)
	}
	assertTaskEvent(t, f.db, f.inv, next, domain.KindIssue)
	graphCounts(t, f.db, 1, 1, 2, 1)
	assertInvoiceUnchanged(t, f)
}

func TestScanner_QueryPendingOrErrorDoesNotReopen(t *testing.T) {
	for _, queryErr := range []bool{false, true} {
		name := "pending"
		if queryErr {
			name = "query error"
		}
		t.Run(name, func(t *testing.T) {
			f, sc := timeoutFixture(t, func(context.Context, *providerv1.QueryOrderRequest) (*providerv1.QueryOrderReply, error) {
				if queryErr {
					return nil, errors.New("supplier unavailable")
				}
				return &providerv1.QueryOrderReply{Found: true, Outcome: providerv1.OrderOutcome_ORDER_OUTCOME_PENDING}, nil
			})
			_, _ = sc.RunOnce(context.Background())
			assertInvoiceUnchanged(t, f)
			graphCounts(t, f.db, 1, 1, 1, 0)
			countRows(t, f.db, `SELECT COUNT(*) FROM tasks WHERE id=? AND status='waiting_callback' AND version=0`, 1, f.task)
			countRows(t, f.db, `SELECT COUNT(*) FROM state_log`, 0)
		})
	}
}

func TestScanner_CallbackWinsDuringQuery(t *testing.T) {
	for _, outcome := range []providerv1.OrderOutcome{providerv1.OrderOutcome_ORDER_OUTCOME_SUCCEEDED, providerv1.OrderOutcome_ORDER_OUTCOME_FAILED} {
		t.Run(outcome.String(), func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			f, sc := timeoutFixture(t, func(ctx context.Context, _ *providerv1.QueryOrderRequest) (*providerv1.QueryOrderReply, error) {
				close(entered)
				select {
				case <-release:
					return &providerv1.QueryOrderReply{Found: true, Outcome: outcome, InvoiceNo: "AIR-A-12345678"}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			})
			done := make(chan error, 1)
			go func() { _, err := sc.RunOnce(ctx); done <- err }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("query did not start")
			}
			env, err := event.New(mq.ProviderTaskSucceeded, "1", 1, event.TaskOutcome{TaskID: f.task, InvoiceID: f.inv.ID, InvoiceNo: "AIR-A-12345678"}, "")
			if err != nil {
				t.Fatal(err)
			}
			err = f.events.Handle(ctx, env)
			close(release)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("scanner blocked")
			}
			assertIssuedOutcome(t, f)
		})
	}
}
