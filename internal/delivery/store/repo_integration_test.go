//go:build integration

package store_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/mafuky/invoice-hub/internal/delivery/store"
	"github.com/mafuky/invoice-hub/internal/delivery/store/storetest"
	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
	platformstore "github.com/mafuky/invoice-hub/internal/platform/store"
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
		if err := repo.InsertOutbox(ctx, mq.DeliverySucceeded, env, ""); err != nil {
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
		return repo.InsertOutbox(ctx, mq.DeliverySucceeded, env, "")
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
	if ok, _, err := repo.MarkDelivering(ctx, d.ID, now); err != nil || !ok {
		t.Fatalf("first claim: %v %v", ok, err)
	}
	if ok, _, err := repo.MarkDelivering(ctx, d.ID, now); err != nil || ok {
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
	if _, _, err := repo.MarkDelivering(ctx, d.ID, now); err != nil {
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

func TestRepo_FencedMarkWithOldClaimReturnsClaimLost(t *testing.T) {
	ctx := context.Background()
	repo := storetest.NewRepo(t)
	if _, err := repo.InsertPending(ctx, customerDelivery(7006)); err != nil {
		t.Fatal(err)
	}
	d, _ := repo.Find(ctx, 7006, 1, store.ChannelCustomer)
	now := time.Now().UTC()

	_, oldSince, err := repo.MarkDelivering(ctx, d.ID, now.Add(-10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := repo.Requeue(ctx, d.ID, now, now); err != nil || !ok {
		t.Fatalf("requeue as stale: %v %v", ok, err)
	}
	ok, newSince, err := repo.MarkDelivering(ctx, d.ID, now)
	if err != nil || !ok {
		t.Fatalf("re-claim: %v %v", ok, err)
	}

	if err := repo.MarkDelivered(ctx, d.ID, oldSince, now); !errors.Is(err, store.ErrClaimLost) {
		t.Fatalf("MarkDelivered with the old claim = %v, want ErrClaimLost", err)
	}
	if err := repo.MarkRetry(ctx, d.ID, oldSince, 1, now, "x"); !errors.Is(err, store.ErrClaimLost) {
		t.Fatalf("MarkRetry with the old claim = %v, want ErrClaimLost", err)
	}
	if err := repo.MarkGaveUp(ctx, d.ID, oldSince, 8, "x"); !errors.Is(err, store.ErrClaimLost) {
		t.Fatalf("MarkGaveUp with the old claim = %v, want ErrClaimLost", err)
	}
	got, err := repo.Get(ctx, d.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != store.StatusDelivering || got.DeliveringSince == nil || !got.DeliveringSince.Equal(newSince) {
		t.Fatalf("record changed by a lost claim: %+v", got)
	}
	if err := repo.MarkDelivered(ctx, d.ID, newSince, now); err != nil {
		t.Fatalf("MarkDelivered with the current claim: %v", err)
	}
}

func TestRepo_RecordChannelOutcome_ConcurrentChannelsAnnounceDeliveredOnce(t *testing.T) {
	ctx := context.Background()
	repo := storetest.NewRepo(t)

	for round := 0; round < 5; round++ {
		invoiceID := int64(7100 + round)
		if err := repo.EnsureInvoiceSummary(ctx, invoiceID, 1); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		var mu sync.Mutex
		announced := 0
		for _, ch := range store.Channels {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var announce bool // from the attempt that committed; a retried attempt overwrites it
				err := repo.WithTx(ctx, func(ctx context.Context) error {
					var gaveUp bool
					var err error
					announce, gaveUp, err = repo.RecordChannelOutcome(ctx, invoiceID, 1, ch, store.OutcomeDelivered)
					if err == nil && gaveUp {
						t.Errorf("round %d: delivered outcome announced gave up", round)
					}
					return err
				})
				if err != nil {
					t.Errorf("round %d channel %s: %v", round, ch, err)
					return
				}
				if announce {
					mu.Lock()
					announced++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if announced != 1 {
			t.Fatalf("round %d: %d transactions announced delivered, want exactly 1", round, announced)
		}
	}
}

func TestRepo_EnsureInvoiceSummary_PreservesAnnouncements(t *testing.T) {
	ctx := context.Background()
	repo := storetest.NewRepo(t)
	for i, ch := range []string{store.ChannelCustomer, store.ChannelFinance, store.ChannelFinance} {
		if err := repo.EnsureInvoiceSummary(ctx, 7201, 1); err != nil {
			t.Fatal(err)
		}
		var delivered, gaveUp bool
		if err := repo.WithTx(ctx, func(ctx context.Context) error {
			var err error
			delivered, gaveUp, err = repo.RecordChannelOutcome(ctx, 7201, 1, ch, store.OutcomeDelivered)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if gaveUp {
			t.Fatal("successful delivery announced gave up")
		}
		// Only the first finance outcome completes both channels.
		if delivered != (i == 1) {
			t.Fatalf("outcome %d announced delivered = %v, want %v", i, delivered, i == 1)
		}
	}
}

func TestRepo_InvoiceSummaryCollectionExistsBeforeDelivery(t *testing.T) {
	ctx := context.Background()
	db, prefix := storetest.Open(t)
	names, err := db.ListCollectionNames(ctx, bson.D{{Key: "name", Value: prefix + "delivery_invoices"}})
	if err != nil || len(names) != 1 {
		t.Fatalf("summary collections = %v (err %v), want one before any delivery", names, err)
	}
	if err := platformstore.EnsureMongoIndexesWithPrefix(ctx, db, prefix); err != nil {
		t.Fatalf("repeated index setup: %v", err)
	}
}
