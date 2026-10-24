//go:build integration

package outbox_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/mafuky/invoice-hub/internal/delivery/outbox"
	"github.com/mafuky/invoice-hub/internal/delivery/store"
	"github.com/mafuky/invoice-hub/internal/delivery/store/storetest"
	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
)

type recordingPub struct {
	mu   sync.Mutex
	ids  []string
	fail error
}

func (p *recordingPub) Publish(_ context.Context, _ string, env event.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail != nil {
		return p.fail
	}
	p.ids = append(p.ids, env.EventID)
	return nil
}

func queue(t *testing.T, repo *store.Repo, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		env, err := event.New(mq.DeliverySucceeded, "8001", 1, event.DeliveryDone{InvoiceID: 8001, InvoiceVersion: 1}, "trace-relay")
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.InsertOutbox(context.Background(), mq.DeliverySucceeded, env, ""); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRelay_TwoRelaysSendEachEventOnce(t *testing.T) {
	ctx := context.Background()
	db, prefix := storetest.Open(t)
	repo := store.NewRepoWithPrefix(db, prefix)
	queue(t, repo, 20)

	pub := &recordingPub{}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := outbox.NewRelay(repo.OutboxCollection(), pub, "delivery").RunOnce(ctx); err != nil {
				t.Errorf("RunOnce: %v", err)
			}
		}()
	}
	wg.Wait()

	seen := map[string]int{}
	for _, id := range pub.ids {
		seen[id]++
	}
	if len(pub.ids) != 20 || len(seen) != 20 {
		t.Fatalf("published %d events, %d distinct; want 20 and 20", len(pub.ids), len(seen))
	}
	if n, _ := repo.OutboxCollection().CountDocuments(ctx, bson.D{{Key: "status", Value: "sent"}}); n != 20 {
		t.Fatalf("sent rows = %d, want 20", n)
	}
}

func TestRelay_FailedPublishKeepsRowPendingAndBacksOff(t *testing.T) {
	ctx := context.Background()
	db, prefix := storetest.Open(t)
	repo := store.NewRepoWithPrefix(db, prefix)
	queue(t, repo, 1)

	relay := outbox.NewRelay(repo.OutboxCollection(), &recordingPub{fail: errors.New("broker nacked")}, "delivery")
	if n, err := relay.RunOnce(ctx); err == nil || n != 0 {
		t.Fatalf("RunOnce = %d, %v; want 0 and an error", n, err)
	}
	var row struct {
		Status   string `bson:"status"`
		Attempts int    `bson:"attempts"`
	}
	if err := repo.OutboxCollection().FindOne(ctx, bson.D{}).Decode(&row); err != nil {
		t.Fatal(err)
	}
	if row.Status != "pending" || row.Attempts != 1 {
		t.Fatalf("row = %+v, want pending with 1 attempt", row)
	}

	// Backed off: an immediate second pass finds nothing due.
	good := &recordingPub{}
	if n, err := outbox.NewRelay(repo.OutboxCollection(), good, "delivery").RunOnce(ctx); err != nil || n != 0 || len(good.ids) != 0 {
		t.Fatalf("second pass = %d, %v, published %d; want nothing", n, err, len(good.ids))
	}
}
