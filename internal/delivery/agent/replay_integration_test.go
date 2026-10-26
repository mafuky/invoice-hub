//go:build lesson && integration

package agent

import (
	"context"
	"reflect"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/mafuky/invoice-hub/internal/delivery/store"
	"github.com/mafuky/invoice-hub/internal/delivery/store/storetest"
)

func TestOnInvoiceIssued_DifferentEventsSameInvoiceVersionRemainIdempotent(t *testing.T) {
	repo := storetest.NewRepo(t)
	ctx, cancel := context.WithTimeout(context.Background(), arrivalTimeout)
	defer cancel()
	a := NewDeliveryAgent(repo, nil)
	first := invoiceIssuedEnvelope(t, 7303, 1)
	second := invoiceIssuedEnvelope(t, 7303, 1)
	if first.EventID == second.EventID {
		t.Fatal("fixture must use distinct event IDs")
	}
	if err := a.OnInvoiceIssued(ctx, first); err != nil {
		t.Fatalf("first event: %v", err)
	}
	before := make(map[string]store.Delivery)
	for _, ch := range store.Channels {
		d, err := repo.Find(ctx, 7303, 1, ch)
		if err != nil {
			t.Fatal(err)
		}
		claimed, since, err := repo.MarkDelivering(ctx, d.ID, time.Now())
		if err != nil || !claimed {
			t.Fatalf("claim %s = %v, %v", ch, claimed, err)
		}
		if ch == store.ChannelCustomer {
			err = repo.MarkDelivered(ctx, d.ID, since, time.Now())
		} else {
			err = repo.MarkGaveUp(ctx, d.ID, since, MaxAttempts, "finance unavailable")
		}
		if err != nil {
			t.Fatal(err)
		}
		before[ch], err = repo.Get(ctx, d.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := a.OnInvoiceIssued(ctx, second); err != nil {
		t.Fatalf("different event for the same invoice version: %v", err)
	}
	if n, err := repo.Deliveries().CountDocuments(ctx, bson.D{}); err != nil || n != 2 {
		t.Fatalf("deliveries = %d, %v; want exactly two", n, err)
	}
	for ch, want := range before {
		got, err := repo.Find(ctx, 7303, 1, ch)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("%s outcome reset: before %+v after %+v", ch, want, got)
		}
	}
	for _, id := range []string{first.EventID, second.EventID} {
		if n, err := repo.ProcessedEvents().CountDocuments(ctx, bson.D{{Key: "event_id", Value: id}}); err != nil || n != 1 {
			t.Fatalf("event %s markers = %d, %v", id, n, err)
		}
	}
}
