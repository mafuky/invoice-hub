//go:build lesson && integration

package app

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"

	providerv1 "github.com/mafuky/invoice-hub/gen/invoicehub/provider/v1"
	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/invoice/store"
	"github.com/mafuky/invoice-hub/internal/platform/money"
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
