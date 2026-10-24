//go:build lesson && integration

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
	"github.com/mafuky/invoice-hub/internal/provider/client"
	"github.com/mafuky/invoice-hub/internal/provider/store"
)

func taskCreatedEnvelope(t *testing.T, taskID, invoiceID int64, line string) event.Envelope {
	t.Helper()
	amt, err := money.Parse("1999.99")
	if err != nil {
		t.Fatal(err)
	}
	env, err := event.New(mq.InvoiceTaskCreated, fmt.Sprint(invoiceID), 1, event.TaskCreated{
		TaskID: taskID, InvoiceID: invoiceID, CommandKind: "ISSUE", ProductLine: line,
		Title: "Example Travel Co., Ltd.", TaxNo: "91310000EXAMPLE001", Amount: amt, CustomerID: "CUST-0001",
	}, "trace-task-created")
	if err != nil {
		t.Fatal(err)
	}
	return env
}

// countingMock answers every order with a fresh supplier order number and counts the calls.
func countingMock(t *testing.T) (*client.Mock, *atomic.Int32) {
	t.Helper()
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"supplier_order_no": fmt.Sprintf("AIR-A-ORD-%06d", n.Add(1))})
	}))
	t.Cleanup(srv.Close)
	return client.NewMock(srv.URL), &n
}

func TestOnTaskCreated_DuplicateEventOrdersOnce(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "provider")
	mock, orders := countingMock(t)
	a := NewProviderAgent(db, store.NewRepo(db), mock, DefaultSuppliers())

	env := taskCreatedEnvelope(t, 501, 301, "FLIGHT")
	for i := 0; i < 2; i++ {
		if err := a.OnTaskCreated(ctx, env); err != nil {
			t.Fatalf("delivery %d: %v", i+1, err)
		}
	}

	if n := orders.Load(); n != 1 {
		t.Fatalf("supplier received %d orders, want 1", n)
	}
	var rows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM provider_orders WHERE task_id = 501`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("provider_orders rows = %d (err %v), want 1", rows, err)
	}
	var submitted int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox WHERE routing_key = ?`, mq.ProviderTaskSubmitted).Scan(&submitted); err != nil || submitted != 1 {
		t.Fatalf("submitted events = %d (err %v), want 1", submitted, err)
	}
}

func TestOnTaskCreated_ReplacementTaskAvoidsFailedSupplier(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "provider")
	repo := store.NewRepo(db)
	mock, _ := countingMock(t)
	a := NewProviderAgent(db, repo, mock, DefaultSuppliers())

	if _, err := repo.InsertOrder(ctx, db, store.NewOrder{TaskID: 601, InvoiceID: 401, Supplier: "air-a", SupplierOrderNo: "AIR-A-ORD-900001"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE provider_orders SET status = ? WHERE task_id = 601`, store.StatusFailed); err != nil {
		t.Fatal(err)
	}

	if err := a.OnTaskCreated(ctx, taskCreatedEnvelope(t, 602, 401, "FLIGHT")); err != nil {
		t.Fatal(err)
	}
	var supplier string
	if err := db.QueryRow(`SELECT supplier FROM provider_orders WHERE task_id = 602`).Scan(&supplier); err != nil || supplier != "backup" {
		t.Fatalf("replacement task went to %q (err %v), want backup", supplier, err)
	}
}
