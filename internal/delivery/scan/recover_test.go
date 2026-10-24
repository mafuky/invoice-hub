//go:build lesson && integration

package scan

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/mafuky/invoice-hub/internal/delivery/store"
	"github.com/mafuky/invoice-hub/internal/delivery/store/storetest"
)

func deliveringSince(t *testing.T, repo *store.Repo, invoiceID int64, since time.Time) store.Delivery {
	t.Helper()
	ctx := context.Background()
	if _, err := repo.InsertPending(ctx, store.NewDelivery{
		InvoiceID: invoiceID, InvoiceVersion: 1, Channel: store.ChannelCustomer,
		CustomerID: "CUST-0001", InvoiceNo: "AIR-A-12345678", Amount: "1999.99",
	}); err != nil {
		t.Fatal(err)
	}
	d, err := repo.Find(ctx, invoiceID, 1, store.ChannelCustomer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Deliveries().UpdateByID(ctx, d.ID, bson.D{{Key: "$set", Value: bson.D{
		{Key: "status", Value: store.StatusDelivering},
		{Key: "delivering_since", Value: since},
	}}}); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestRecoverStuck_OnlyOldDelivering(t *testing.T) {
	ctx := context.Background()
	repo := storetest.NewRepo(t)
	now := time.Now().UTC()
	old := deliveringSince(t, repo, 7201, now.Add(-10*time.Minute))
	fresh := deliveringSince(t, repo, 7202, now.Add(-time.Second))

	n, err := NewScanner(repo).RecoverStuck(ctx, time.Minute)
	if err != nil || n != 1 {
		t.Fatalf("RecoverStuck = %d, %v; want 1, nil", n, err)
	}

	got, err := repo.Get(ctx, old.ID)
	if err != nil || got.Status != store.StatusPending {
		t.Fatalf("old record = %+v (err %v), want pending", got, err)
	}
	got, err = repo.Get(ctx, fresh.ID)
	if err != nil || got.Status != store.StatusDelivering {
		t.Fatalf("recent record = %+v (err %v), want still delivering", got, err)
	}
}
