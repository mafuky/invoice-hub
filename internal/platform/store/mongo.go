package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// OpenMongo connects using the driver's URI handling and pings with a 5s timeout.
func OpenMongo(ctx context.Context, uri, db string) (*mongo.Database, error) {
	return openMongo(ctx, uri, db, func(uri string) (*mongo.Client, error) {
		return mongo.Connect(options.Client().ApplyURI(uri))
	})
}

func openMongo(ctx context.Context, uri, db string, connect func(string) (*mongo.Client, error)) (*mongo.Database, error) {
	client, err := connect(uri)
	if err != nil {
		return nil, fmt.Errorf("store: connect mongo failed; check URI settings and %s", tunnelHint)
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pctx, readpref.Primary()); err != nil {
		dctx, disconnectCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer disconnectCancel()
		_ = client.Disconnect(dctx)
		return nil, fmt.Errorf("store: ping mongo failed; check connectivity and %s", tunnelHint)
	}
	return client.Database(db), nil
}

// EnsureMongoIndexes creates the collections' indexes. It is idempotent:
// CreateMany with an identical spec is a no-op on the server.
func EnsureMongoIndexes(ctx context.Context, db *mongo.Database) error {
	return ensureIndexes(ctx, db, "")
}

// EnsureMongoIndexesWithPrefix creates the same indexes on collections whose names
// carry prefix, so tests can work in collections of their own.
func EnsureMongoIndexesWithPrefix(ctx context.Context, db *mongo.Database, prefix string) error {
	return ensureIndexes(ctx, db, prefix)
}

func ensureIndexes(ctx context.Context, db *mongo.Database, prefix string) error {
	if err := db.CreateCollection(ctx, prefix+"delivery_invoices"); err != nil {
		var commandErr mongo.CommandError
		if !errors.As(err, &commandErr) || commandErr.Code != 48 {
			return fmt.Errorf("store: create delivery invoice summaries: %w", err)
		}
	}
	uniq := func(name string, keys bson.D) mongo.IndexModel {
		return mongo.IndexModel{Keys: keys, Options: options.Index().SetName(name).SetUnique(true)}
	}
	plain := func(name string, keys bson.D) mongo.IndexModel {
		return mongo.IndexModel{Keys: keys, Options: options.Index().SetName(name)}
	}
	specs := []struct {
		coll   string
		models []mongo.IndexModel
	}{
		{"callbacks", []mongo.IndexModel{
			uniq("uk_callbacks_supplier_order_seq", bson.D{{Key: "supplier", Value: 1}, {Key: "supplier_order_no", Value: 1}, {Key: "event_seq", Value: 1}}),
		}},
		{"deliveries", []mongo.IndexModel{
			uniq("uk_deliveries_invoice_channel_version", bson.D{{Key: "invoice_id", Value: 1}, {Key: "channel", Value: 1}, {Key: "invoice_version", Value: 1}}),
			plain("idx_deliveries_status_retry", bson.D{{Key: "status", Value: 1}, {Key: "next_retry_at", Value: 1}}),
			plain("idx_deliveries_status_since", bson.D{{Key: "status", Value: 1}, {Key: "delivering_since", Value: 1}}),
		}},
		{"outbox", []mongo.IndexModel{
			uniq("uk_outbox_event", bson.D{{Key: "event_id", Value: 1}}),
			plain("idx_outbox_status_next", bson.D{{Key: "status", Value: 1}, {Key: "next_attempt_at", Value: 1}}),
			{
				Keys: bson.D{{Key: "dedupe_key", Value: 1}},
				Options: options.Index().SetName("uk_outbox_dedupe_key").SetUnique(true).
					SetPartialFilterExpression(bson.D{{Key: "dedupe_key", Value: bson.D{{Key: "$type", Value: "string"}}}}),
			},
		}},
		{"processed_events", []mongo.IndexModel{
			uniq("uk_processed_events_event", bson.D{{Key: "event_id", Value: 1}}),
		}},
	}
	for _, s := range specs {
		if _, err := db.Collection(prefix+s.coll).Indexes().CreateMany(ctx, s.models); err != nil {
			return fmt.Errorf("store: indexes on %s: %w", prefix+s.coll, err)
		}
	}
	return nil
}
