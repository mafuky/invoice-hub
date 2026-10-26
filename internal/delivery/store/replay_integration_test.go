//go:build integration

package store_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/mafuky/invoice-hub/internal/delivery/store"
	"github.com/mafuky/invoice-hub/internal/delivery/store/storetest"
)

func TestRepo_MarkProcessed_DuplicateInTransactionReturnsFalse(t *testing.T) {
	repo := storetest.NewRepo(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if first, err := repo.MarkProcessed(ctx, "issued-replay"); err != nil || !first {
		t.Fatalf("first marker = %v, %v", first, err)
	}
	var before bson.M
	if err := repo.ProcessedEvents().FindOne(ctx, bson.D{{Key: "event_id", Value: "issued-replay"}}).Decode(&before); err != nil {
		t.Fatal(err)
	}
	err := repo.WithTx(ctx, func(ctx context.Context) error {
		first, err := repo.MarkProcessed(ctx, "issued-replay")
		if err != nil {
			return err
		}
		if first {
			return fmt.Errorf("duplicate event reported as first")
		}
		inserted, err := repo.InsertPending(ctx, customerDelivery(7301))
		if err == nil && !inserted {
			return fmt.Errorf("new delivery not inserted")
		}
		return err
	})
	if err != nil {
		t.Fatalf("duplicate marker transaction: %v", err)
	}
	if n, err := repo.ProcessedEvents().CountDocuments(ctx, bson.D{}); err != nil || n != 1 {
		t.Fatalf("markers = %d, %v", n, err)
	}
	var after bson.M
	if err := repo.ProcessedEvents().FindOne(ctx, bson.D{{Key: "event_id", Value: "issued-replay"}}).Decode(&after); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("duplicate changed the original marker")
	}
	if _, err := repo.Find(ctx, 7301, 1, store.ChannelCustomer); err != nil {
		t.Fatalf("other transaction write did not commit: %v", err)
	}
}

func TestRepo_InsertPending_DuplicateInTransactionPreservesRecordAndCommitsOtherWork(t *testing.T) {
	repo := storetest.NewRepo(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	nd := customerDelivery(7302)
	if inserted, err := repo.InsertPending(ctx, nd); err != nil || !inserted {
		t.Fatalf("first delivery = %v, %v", inserted, err)
	}
	d, err := repo.Find(ctx, nd.InvoiceID, nd.InvoiceVersion, nd.Channel)
	if err != nil {
		t.Fatal(err)
	}
	claimed, since, err := repo.MarkDelivering(ctx, d.ID, time.Now())
	if err != nil || !claimed {
		t.Fatalf("claim = %v, %v", claimed, err)
	}
	if err := repo.MarkDelivered(ctx, d.ID, since, time.Now()); err != nil {
		t.Fatal(err)
	}
	before, err := repo.Get(ctx, d.ID)
	if err != nil {
		t.Fatal(err)
	}
	nd.Amount, nd.InvoiceNo, nd.CustomerID = "100.10", "CHANGED", "CUST-0002"
	err = repo.WithTx(ctx, func(ctx context.Context) error {
		inserted, err := repo.InsertPending(ctx, nd)
		if err != nil {
			return err
		}
		if inserted {
			return fmt.Errorf("duplicate delivery reported as inserted")
		}
		first, err := repo.MarkProcessed(ctx, "issued-other-event")
		if err == nil && !first {
			return fmt.Errorf("new marker not inserted")
		}
		return err
	})
	if err != nil {
		t.Fatalf("duplicate delivery transaction: %v", err)
	}
	after, err := repo.Get(ctx, d.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("existing delivery changed: before %+v after %+v", before, after)
	}
	filter := bson.D{{Key: "invoice_id", Value: nd.InvoiceID}, {Key: "invoice_version", Value: nd.InvoiceVersion}, {Key: "channel", Value: nd.Channel}}
	if n, err := repo.Deliveries().CountDocuments(ctx, filter); err != nil || n != 1 {
		t.Fatalf("identity rows = %d, %v", n, err)
	}
	if n, err := repo.ProcessedEvents().CountDocuments(ctx, bson.D{{Key: "event_id", Value: "issued-other-event"}}); err != nil || n != 1 {
		t.Fatalf("other transaction write = %d markers, %v", n, err)
	}
}
