package store

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/mafuky/invoice-hub/internal/platform/dial"
)

// OpenMongo checks reachability, connects and pings with a 5s timeout.
func OpenMongo(ctx context.Context, uri, db string) (*mongo.Database, error) {
	u, err := url.Parse(uri)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("store: invalid mongo uri")
	}
	if err := dial.Check(u.Host, 5*time.Second, tunnelHint); err != nil {
		return nil, fmt.Errorf("store: mongo: %w", err)
	}

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("store: connect mongo %s: connect failed", u.Host)
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("store: ping mongo %s: %w", u.Host, err)
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
		}},
		{"outbox", []mongo.IndexModel{
			uniq("uk_outbox_event", bson.D{{Key: "event_id", Value: 1}}),
			plain("idx_outbox_status_next", bson.D{{Key: "status", Value: 1}, {Key: "next_attempt_at", Value: 1}}),
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
