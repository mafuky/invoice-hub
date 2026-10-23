//go:build integration

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/mafuky/invoice-hub/internal/platform/config"
)

func loadCfg(t *testing.T) config.Config {
	t.Helper()
	cfg, err := config.Load()
	if err != nil {
		t.Skipf("no config (copy .env.example to .env and run make tunnel): %v", err)
	}
	return cfg
}

func runID() string { return fmt.Sprintf("%08x", rand.Uint32()) }

// The app user may only touch invoice, provider and settlement, so the real
// databases are used, and only when they are empty or already migrated.
func TestMigrate_UpDownUp(t *testing.T) {
	cfg := loadCfg(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	sets := []struct{ name, dsn string }{
		{"invoice", cfg.MySQLDSNInvoice},
		{"provider", cfg.MySQLDSNProvider},
		{"settlement", cfg.MySQLDSNSettlement},
	}
	for _, s := range sets {
		t.Run(s.name, func(t *testing.T) {
			db, err := OpenMySQL(ctx, s.dsn)
			if err != nil {
				t.Skipf("mysql unavailable: %v", err)
			}
			defer db.Close()

			tables := listTables(t, ctx, db)
			wasEmpty := len(tables) == 0
			if !wasEmpty {
				if !tables["schema_migrations"] {
					t.Skipf("database %s holds tables not created by migrations; not touching it", s.name)
				}
				var dirty bool
				if err := db.QueryRowContext(ctx, "SELECT dirty FROM schema_migrations LIMIT 1").Scan(&dirty); err != nil || dirty {
					t.Skipf("database %s has an unusable schema_migrations state; not touching it", s.name)
				}
			}

			must := func(err error, what string) {
				t.Helper()
				if err != nil {
					t.Fatalf("%s: %v", what, err)
				}
			}
			must(MigrateMySQL(db, s.name), "up")
			got := listTables(t, ctx, db)
			for _, want := range []string{"outbox", "processed_events"} {
				if !got[want] {
					t.Errorf("table %s missing after up", want)
				}
			}
			must(MigrateMySQLDown(db, s.name), "down")
			if after := listTables(t, ctx, db); after["outbox"] {
				t.Error("outbox still present after down")
			}
			must(MigrateMySQL(db, s.name), "second up")
			if !listTables(t, ctx, db)["outbox"] {
				t.Error("outbox missing after second up")
			}

			if wasEmpty {
				must(MigrateMySQLDown(db, s.name), "cleanup down")
				if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS schema_migrations"); err != nil {
					t.Fatalf("cleanup: %v", err)
				}
			}
		})
	}
}

func listTables(t *testing.T, ctx context.Context, db *sql.DB) map[string]bool {
	t.Helper()
	rows, err := db.QueryContext(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()")
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		out[n] = true
	}
	return out
}

// The app user has readWrite on invoicehub only, so tests create their
// collections inside it under a random prefix and drop them afterwards.
func mongoSandbox(t *testing.T) (context.Context, *mongo.Database, string) {
	t.Helper()
	cfg := loadCfg(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	db, err := OpenMongo(ctx, cfg.MongoURI, cfg.MongoDB)
	if err != nil {
		t.Skipf("mongo unavailable: %v", err)
	}
	prefix := "t" + runID() + "_"
	if err := ensureIndexes(ctx, db, prefix); err != nil {
		t.Fatalf("ensure indexes: %v", err)
	}
	t.Cleanup(func() {
		dctx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		for _, n := range []string{"callbacks", "deliveries", "outbox", "processed_events"} {
			_ = db.Collection(prefix + n).Drop(dctx)
		}
		_ = db.Client().Disconnect(dctx)
	})
	return ctx, db, prefix
}

func TestMongoIndexes_UniqueCallbackRejectsDuplicate(t *testing.T) {
	ctx, db, p := mongoSandbox(t)
	// Idempotent: a second run must not fail.
	if err := ensureIndexes(ctx, db, p); err != nil {
		t.Fatalf("second ensure: %v", err)
	}
	c := db.Collection(p + "callbacks")
	doc := bson.D{{Key: "supplier", Value: "s1"}, {Key: "supplier_order_no", Value: "o1"}, {Key: "event_seq", Value: 1}}
	if _, err := c.InsertOne(ctx, doc); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	_, err := c.InsertOne(ctx, bson.D{{Key: "supplier", Value: "s1"}, {Key: "supplier_order_no", Value: "o1"}, {Key: "event_seq", Value: 1}})
	if !mongo.IsDuplicateKeyError(err) {
		t.Fatalf("want duplicate key error, got %v", err)
	}
}

func TestMongo_TransactionCommits(t *testing.T) {
	ctx, db, p := mongoSandbox(t)
	sess, err := db.Client().StartSession()
	if err != nil {
		t.Fatal(err)
	}
	defer sess.EndSession(ctx)

	_, err = sess.WithTransaction(ctx, func(sc context.Context) (any, error) {
		if _, err := db.Collection(p+"deliveries").InsertOne(sc, bson.D{
			{Key: "invoice_id", Value: 1}, {Key: "channel", Value: "email"}, {Key: "invoice_version", Value: 1}, {Key: "status", Value: "new"}}); err != nil {
			return nil, err
		}
		return db.Collection(p+"outbox").InsertOne(sc, bson.D{
			{Key: "event_id", Value: "evt-" + p}, {Key: "status", Value: "new"}})
	})
	if err != nil {
		var cmdErr mongo.CommandError
		if errors.As(err, &cmdErr) && cmdErr.Code == 20 {
			t.Skip("mongo is not a replica set; transactions unavailable")
		}
		t.Fatalf("transaction: %v", err)
	}
	for _, n := range []string{"deliveries", "outbox"} {
		if cnt, err := db.Collection(p+n).CountDocuments(ctx, bson.D{}); err != nil || cnt != 1 {
			t.Errorf("%s: count=%d err=%v", n, cnt, err)
		}
	}
}
