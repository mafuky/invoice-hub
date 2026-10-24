//go:build integration

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
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

var migrationSets = func(cfg config.Config) []struct{ name, dsn string } {
	return []struct{ name, dsn string }{
		{"invoice", cfg.MySQLDSNInvoice},
		{"provider", cfg.MySQLDSNProvider},
		{"settlement", cfg.MySQLDSNSettlement},
	}
}

// The app user may only touch invoice, provider and settlement, so the real
// databases are used. The down/up cycle runs only when every table is empty;
// a database holding rows only gets a no-op Up.
func TestMigrate_UpDownUp(t *testing.T) {
	cfg := loadCfg(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	for _, s := range migrationSets(cfg) {
		t.Run(s.name, func(t *testing.T) {
			db, err := OpenMySQL(ctx, s.dsn)
			if err != nil {
				t.Skipf("mysql unavailable: %v", err)
			}
			t.Cleanup(func() { db.Close() }) // registered first, so it runs after the teardown below

			tables := listTables(t, ctx, db)
			wasEmpty := len(tables) == 0
			if !wasEmpty && !tables["schema_migrations"] {
				t.Skipf("database %s holds tables not created by migrations; not touching it", s.name)
			}
			if !wasEmpty {
				var dirty bool
				if err := db.QueryRowContext(ctx, "SELECT dirty FROM schema_migrations LIMIT 1").Scan(&dirty); err != nil || dirty {
					t.Skipf("database %s has an unusable schema_migrations state; not touching it", s.name)
				}
			}

			if hasRows(t, ctx, db, tables) {
				if err := MigrateMySQL(ctx, s.dsn, s.name); err != nil {
					t.Fatalf("up on migrated database: %v", err)
				}
				t.Logf("%s: down/up skipped because the database has data", s.name)
				return
			}

			// Leave the database as found: empty stays empty, migrated stays migrated.
			t.Cleanup(func() {
				cctx, c := context.WithTimeout(context.Background(), time.Minute)
				defer c()
				if wasEmpty {
					if err := MigrateMySQLDown(cctx, s.dsn, s.name); err != nil {
						t.Errorf("cleanup down: %v", err)
					}
					if _, err := db.ExecContext(cctx, "DROP TABLE IF EXISTS schema_migrations"); err != nil {
						t.Errorf("cleanup drop schema_migrations: %v", err)
					}
					return
				}
				if err := MigrateMySQL(cctx, s.dsn, s.name); err != nil {
					t.Errorf("cleanup up: %v", err)
				}
			})

			must := func(err error, what string) {
				t.Helper()
				if err != nil {
					t.Fatalf("%s: %v", what, err)
				}
			}
			must(MigrateMySQL(ctx, s.dsn, s.name), "up")
			got := listTables(t, ctx, db)
			for _, want := range []string{"outbox", "processed_events"} {
				if !got[want] {
					t.Errorf("table %s missing after up", want)
				}
			}
			must(MigrateMySQLDown(ctx, s.dsn, s.name), "down")
			if listTables(t, ctx, db)["outbox"] {
				t.Error("outbox still present after down")
			}
			must(MigrateMySQL(ctx, s.dsn, s.name), "second up")
			if !listTables(t, ctx, db)["outbox"] {
				t.Error("outbox missing after second up")
			}
		})
	}
}

func hasRows(t *testing.T, ctx context.Context, db *sql.DB, tables map[string]bool) bool {
	t.Helper()
	for n := range tables {
		if n == "schema_migrations" {
			continue
		}
		var one int
		err := db.QueryRowContext(ctx, "SELECT 1 FROM `"+n+"` LIMIT 1").Scan(&one)
		if err == nil {
			return true
		}
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("probe %s: %v", n, err)
		}
	}
	return false
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

// migratedInvoiceDB returns the invoice database with the schema applied,
// skipping if it holds tables this repo did not create.
func migratedInvoiceDB(t *testing.T) (context.Context, *sql.DB) {
	t.Helper()
	cfg := loadCfg(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	db, err := OpenMySQL(ctx, cfg.MySQLDSNInvoice)
	if err != nil {
		t.Skipf("mysql unavailable: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if tables := listTables(t, ctx, db); len(tables) > 0 && !tables["schema_migrations"] {
		t.Skip("invoice database holds tables not created by migrations; not touching it")
	}
	if err := MigrateMySQL(ctx, cfg.MySQLDSNInvoice, "invoice"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return ctx, db
}

// CURRENT_TIMESTAMP defaults follow the session zone, which OpenMySQL pins to UTC.
func TestMySQL_DefaultTimestampIsUTC(t *testing.T) {
	ctx, db := migratedInvoiceDB(t)
	id := "tz-" + runID()
	t.Cleanup(func() { db.ExecContext(context.Background(), "DELETE FROM outbox WHERE event_id = ?", id) })

	if _, err := db.ExecContext(ctx, "INSERT INTO outbox (event_id, routing_key, body) VALUES (?, 'test.tz', '{}')", id); err != nil {
		t.Fatal(err)
	}
	var created time.Time
	if err := db.QueryRowContext(ctx, "SELECT created_at FROM outbox WHERE event_id = ?", id).Scan(&created); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(created); d > 5*time.Second || d < -5*time.Second {
		t.Fatalf("created_at %v is %v away from now (%v)", created, d, time.Now().UTC())
	}
}

func TestMySQL_BizOrderNoUniqueAndCaseSensitive(t *testing.T) {
	ctx, db := migratedInvoiceDB(t)
	no := "BIZ-" + runID()
	t.Cleanup(func() {
		db.ExecContext(context.Background(), "DELETE FROM invoices WHERE biz_order_no IN (?, ?)", no, strings.ToLower(no))
	})
	ins := func(n string) error {
		_, err := db.ExecContext(ctx, `INSERT INTO invoices (biz_order_no, product_line, title, tax_no, amount, customer_id, status)
			VALUES (?, 'saas', 'title', 'tax', 100.10, 'c1', 'PENDING')`, n)
		return err
	}
	if err := ins(no); err != nil {
		t.Fatal(err)
	}
	var me *mysql.MySQLError
	if err := ins(no); !errors.As(err, &me) || me.Number != 1062 {
		t.Fatalf("want error 1062, got %v", err)
	}
	if err := ins(strings.ToLower(no)); err != nil {
		t.Fatalf("a different-case order number must be distinct: %v", err)
	}
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
