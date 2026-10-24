//go:build lesson && integration

package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/mafuky/invoice-hub/internal/delivery/channel"
	"github.com/mafuky/invoice-hub/internal/delivery/scan"
	"github.com/mafuky/invoice-hub/internal/delivery/store"
	"github.com/mafuky/invoice-hub/internal/delivery/store/storetest"
	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
)

func invoiceIssuedEnvelope(t *testing.T, invoiceID int64, version int) event.Envelope {
	t.Helper()
	amt, err := money.Parse("1999.99")
	if err != nil {
		t.Fatal(err)
	}
	env, err := event.New(mq.InvoiceIssued, "7100", version, event.InvoiceIssued{
		InvoiceID: invoiceID, Version: version, InvoiceNo: "AIR-A-12345678", CustomerID: "CUST-0001",
		ProductLine: "FLIGHT", Title: "Example Travel Co., Ltd.", Amount: amt,
	}, "trace-issued")
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func TestOnInvoiceIssued_TwiceCreatesTwoDeliveriesOnly(t *testing.T) {
	ctx := context.Background()
	repo := storetest.NewRepo(t)
	a := NewDeliveryAgent(repo, nil)

	env := invoiceIssuedEnvelope(t, 7101, 1)
	for i := 0; i < 2; i++ {
		if err := a.OnInvoiceIssued(ctx, env); err != nil {
			t.Fatalf("delivery %d of the event: %v", i+1, err)
		}
	}

	if n, err := repo.Deliveries().CountDocuments(ctx, bson.D{{Key: "invoice_id", Value: int64(7101)}}); err != nil || n != 2 {
		t.Fatalf("deliveries = %d (err %v), want 2", n, err)
	}
	for _, ch := range store.Channels {
		d, err := repo.Find(ctx, 7101, 1, ch)
		if err != nil {
			t.Fatalf("channel %s: %v", ch, err)
		}
		if d.Status != store.StatusPending {
			t.Fatalf("channel %s status = %s, want pending", ch, d.Status)
		}
	}
}

// receiver stands in for the customer endpoint. The first push is recorded and
// then answered too late; later pushes are answered at once.
type receiver struct {
	mu     sync.Mutex
	pushes []map[string]any
}

func (r *receiver) handler(hold time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var b map[string]any
		_ = json.NewDecoder(req.Body).Decode(&b)
		r.mu.Lock()
		r.pushes = append(r.pushes, b)
		first := len(r.pushes) == 1
		r.mu.Unlock()
		if first {
			select {
			case <-time.After(hold):
			case <-req.Context().Done():
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	})
}

func TestDeliver_SlowSuccessDoesNotDoublePushAfterRecover(t *testing.T) {
	ctx := context.Background()
	repo := storetest.NewRepo(t)

	rcv := &receiver{}
	srv := httptest.NewServer(rcv.handler(2 * time.Second))
	t.Cleanup(srv.Close)
	a := NewDeliveryAgent(repo, map[string]Sender{
		store.ChannelCustomer: channel.NewCustomer(srv.URL, 200*time.Millisecond),
	})

	if _, err := repo.InsertPending(ctx, store.NewDelivery{
		InvoiceID: 7102, InvoiceVersion: 1, Channel: store.ChannelCustomer,
		CustomerID: "CUST-0001", InvoiceNo: "AIR-A-12345678", Amount: "1999.99",
	}); err != nil {
		t.Fatal(err)
	}
	d, err := repo.Find(ctx, 7102, 1, store.ChannelCustomer)
	if err != nil {
		t.Fatal(err)
	}

	if err := a.Deliver(ctx, d); err == nil {
		t.Fatal("first Deliver succeeded although the customer endpoint answered too late")
	}

	// The worker is gone before the outcome could be recorded: the record
	// is left in delivering from ten minutes ago.
	if _, err := repo.Deliveries().UpdateByID(ctx, d.ID, bson.D{{Key: "$set", Value: bson.D{
		{Key: "status", Value: store.StatusDelivering},
		{Key: "delivering_since", Value: time.Now().UTC().Add(-10 * time.Minute)},
	}}}); err != nil {
		t.Fatal(err)
	}
	if n, err := scan.NewScanner(repo).RecoverStuck(ctx, time.Minute); err != nil || n != 1 {
		t.Fatalf("RecoverStuck = %d, %v; want 1, nil", n, err)
	}

	d, err = repo.Find(ctx, 7102, 1, store.ChannelCustomer)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Deliver(ctx, d); err != nil {
		t.Fatalf("second Deliver: %v", err)
	}

	// At least once: the customer saw the invoice twice, both times with the
	// identity it needs to drop the repeat.
	rcv.mu.Lock()
	defer rcv.mu.Unlock()
	if len(rcv.pushes) != 2 {
		t.Fatalf("customer received %d pushes, want 2", len(rcv.pushes))
	}
	for i, p := range rcv.pushes {
		if p["invoice_id"] != "7102" || p["invoice_version"] != float64(1) {
			t.Fatalf("push %d = %v, want invoice_id 7102 and invoice_version 1", i+1, p)
		}
	}
	if n, err := repo.Deliveries().CountDocuments(ctx, bson.D{{Key: "invoice_id", Value: int64(7102)}}); err != nil || n != 1 {
		t.Fatalf("delivery records = %d (err %v), want 1", n, err)
	}
	got, err := repo.Find(ctx, 7102, 1, store.ChannelCustomer)
	if err != nil || got.Status != store.StatusDelivered {
		t.Fatalf("final record = %+v (err %v), want delivered", got, err)
	}
}
