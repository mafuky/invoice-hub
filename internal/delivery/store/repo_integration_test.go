//go:build integration

package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/mafuky/invoice-hub/internal/delivery/store"
	"github.com/mafuky/invoice-hub/internal/delivery/store/storetest"
	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
)

func customerDelivery(invoiceID int64) store.NewDelivery {
	return store.NewDelivery{
		InvoiceID: invoiceID, InvoiceVersion: 1, Channel: store.ChannelCustomer,
		CustomerID: "CUST-0001", InvoiceNo: "AIR-A-12345678", Amount: "1999.99",
	}
}

func TestRepo_WithTxRollsBackBoth(t *testing.T) {
	ctx := context.Background()
	repo := storetest.NewRepo(t)
	boom := errors.New("settlement refused")

	err := repo.WithTx(ctx, func(ctx context.Context) error {
		if _, err := repo.InsertPending(ctx, customerDelivery(7001)); err != nil {
			return err
		}
		env, err := event.New(mq.DeliverySucceeded, "7001", 1, event.DeliveryDone{InvoiceID: 7001, InvoiceVersion: 1}, "trace-tx")
		if err != nil {
			return err
		}
		if err := repo.InsertOutbox(ctx, mq.DeliverySucceeded, env); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("WithTx = %v, want the callback's error", err)
	}

	if n, err := repo.Deliveries().CountDocuments(ctx, bson.D{}); err != nil || n != 0 {
		t.Fatalf("deliveries = %d (err %v), want 0 after rollback", n, err)
	}
	if n, err := repo.OutboxCollection().CountDocuments(ctx, bson.D{}); err != nil || n != 0 {
		t.Fatalf("outbox = %d (err %v), want 0 after rollback", n, err)
	}
}

func TestRepo_WithTxCommitsBoth(t *testing.T) {
	ctx := context.Background()
	repo := storetest.NewRepo(t)

	err := repo.WithTx(ctx, func(ctx context.Context) error {
		if _, err := repo.InsertPending(ctx, customerDelivery(7002)); err != nil {
			return err
		}
		env, err := event.New(mq.DeliverySucceeded, "7002", 1, event.DeliveryDone{InvoiceID: 7002, InvoiceVersion: 1}, "trace-tx")
		if err != nil {
			return err
		}
		return repo.InsertOutbox(ctx, mq.DeliverySucceeded, env)
	})
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := repo.Deliveries().CountDocuments(ctx, bson.D{}); n != 1 {
		t.Fatalf("deliveries = %d, want 1", n)
	}
	if n, _ := repo.OutboxCollection().CountDocuments(ctx, bson.D{}); n != 1 {
		t.Fatalf("outbox = %d, want 1", n)
	}
}

func TestRepo_InsertPending_SameInvoiceChannelVersionOnce(t *testing.T) {
	ctx := context.Background()
	repo := storetest.NewRepo(t)

	if ins, err := repo.InsertPending(ctx, customerDelivery(7003)); err != nil || !ins {
		t.Fatalf("first insert: inserted=%v err=%v", ins, err)
	}
	if ins, err := repo.InsertPending(ctx, customerDelivery(7003)); err != nil || ins {
		t.Fatalf("repeat insert: inserted=%v err=%v, want false and no error", ins, err)
	}
	next := customerDelivery(7003)
	next.InvoiceVersion = 2
	if ins, err := repo.InsertPending(ctx, next); err != nil || !ins {
		t.Fatalf("reissued version: inserted=%v err=%v, want a new delivery", ins, err)
	}
}

func TestRepo_MarkDelivering_OnlyOneWorkerClaims(t *testing.T) {
	ctx := context.Background()
	repo := storetest.NewRepo(t)
	if _, err := repo.InsertPending(ctx, customerDelivery(7004)); err != nil {
		t.Fatal(err)
	}
	d, err := repo.Find(ctx, 7004, 1, store.ChannelCustomer)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if ok, err := repo.MarkDelivering(ctx, d.ID, now); err != nil || !ok {
		t.Fatalf("first claim: %v %v", ok, err)
	}
	if ok, err := repo.MarkDelivering(ctx, d.ID, now); err != nil || ok {
		t.Fatalf("second claim: %v %v, want false", ok, err)
	}
}

func TestRepo_RequeueKeepsRecentDelivering(t *testing.T) {
	ctx := context.Background()
	repo := storetest.NewRepo(t)
	if _, err := repo.InsertPending(ctx, customerDelivery(7005)); err != nil {
		t.Fatal(err)
	}
	d, _ := repo.Find(ctx, 7005, 1, store.ChannelCustomer)
	now := time.Now().UTC()
	if _, err := repo.MarkDelivering(ctx, d.ID, now); err != nil {
		t.Fatal(err)
	}

	if ok, err := repo.Requeue(ctx, d.ID, now.Add(-time.Minute), now); err != nil || ok {
		t.Fatalf("requeue of a fresh delivering record: %v %v, want false", ok, err)
	}
	if ok, err := repo.Requeue(ctx, d.ID, now.Add(time.Minute), now); err != nil || !ok {
		t.Fatalf("requeue of an old delivering record: %v %v, want true", ok, err)
	}
	got, _ := repo.Get(ctx, d.ID)
	if got.Status != store.StatusPending || got.DeliveringSince != nil {
		t.Fatalf("after requeue: status=%s since=%v", got.Status, got.DeliveringSince)
	}
}
