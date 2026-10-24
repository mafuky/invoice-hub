//go:build lesson && integration

package app

import (
	"context"
	"sync"
	"testing"

	"github.com/mafuky/invoice-hub/internal/invoice/store"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
)

func TestApply_ConcurrentSameBizOrderNo_OneInvoice(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "invoice")
	svc := NewService(db, store.NewRepo(db))

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
}

func TestApply_NewRequest_CreatesCommandPendingTaskAndEvent(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "invoice")
	svc := NewService(db, store.NewRepo(db))

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
}
