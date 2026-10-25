//go:build integration

package store

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/mafuky/invoice-hub/internal/platform/config"
	platformstore "github.com/mafuky/invoice-hub/internal/platform/store"
)

const arrivalTimeout = 60 * time.Second

func TestRawCallbacks_RepeatedPayloadReportsNotInserted(t *testing.T) {
	cfg, err := config.Load()
	if err != nil {
		t.Skipf("no config: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), arrivalTimeout)
	defer cancel()
	db, err := platformstore.OpenMongo(ctx, cfg.MongoURI, cfg.MongoDB)
	if err != nil {
		t.Skipf("mongo unavailable: %v", err)
	}
	name := fmt.Sprintf("t%08x_callbacks", rand.Uint32())
	coll := db.Collection(name)
	t.Cleanup(func() {
		dctx, c := context.WithTimeout(context.Background(), arrivalTimeout)
		defer c()
		_ = coll.Drop(dctx)
	})
	if _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "supplier", Value: 1}, {Key: "supplier_order_no", Value: 1}, {Key: "event_seq", Value: 1}},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		t.Fatal(err)
	}

	raw := NewRawCallbacks(coll)
	cb := RawCallback{Supplier: "air-a", SupplierOrderNo: "AIR-A-ORD-000777", EventSeq: 1, Outcome: "SUCCEEDED", InvoiceNo: "AIR-A-12345678", RawJSON: `{"event_seq":1}`}
	if ins, err := raw.Store(ctx, cb); err != nil || !ins {
		t.Fatalf("first store: inserted=%v err=%v", ins, err)
	}
	if ins, err := raw.Store(ctx, cb); err != nil || ins {
		t.Fatalf("repeat store: inserted=%v err=%v, want false and no error", ins, err)
	}
	cb.EventSeq = 2
	if ins, err := raw.Store(ctx, cb); err != nil || !ins {
		t.Fatalf("next event_seq: inserted=%v err=%v", ins, err)
	}
}
