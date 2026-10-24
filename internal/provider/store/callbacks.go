package store

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// RawCallbacks keeps every supplier callback exactly as it arrived. The unique
// index on (supplier, supplier_order_no, event_seq) tells a new payload from a repeat.
type RawCallbacks struct {
	coll *mongo.Collection
}

func NewRawCallbacks(coll *mongo.Collection) *RawCallbacks { return &RawCallbacks{coll: coll} }

// RawCallback is one stored payload.
type RawCallback struct {
	Supplier        string
	SupplierOrderNo string
	EventSeq        int64
	Outcome         string
	InvoiceNo       string
	RawJSON         string
}

// Store inserts the payload and reports inserted=false when the same event_seq
// of the same supplier order is already there.
func (c *RawCallbacks) Store(ctx context.Context, cb RawCallback) (inserted bool, err error) {
	_, err = c.coll.InsertOne(ctx, bson.D{
		{Key: "supplier", Value: cb.Supplier},
		{Key: "supplier_order_no", Value: cb.SupplierOrderNo},
		{Key: "event_seq", Value: cb.EventSeq},
		{Key: "outcome", Value: cb.Outcome},
		{Key: "invoice_no", Value: cb.InvoiceNo},
		{Key: "raw_json", Value: cb.RawJSON},
		{Key: "received_at", Value: time.Now().UTC()},
	})
	if mongo.IsDuplicateKeyError(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store raw callback %s/%s seq %d: %w", cb.Supplier, cb.SupplierOrderNo, cb.EventSeq, err)
	}
	return true, nil
}
