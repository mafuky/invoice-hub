//go:build integration

// Package testenv gives integration tests a migrated, dedicated MySQL database
// (invoice_test, provider_test or settlement_test) derived from the local config.
// The live databases are never touched.
//
// Packages run as separate processes and share the test database, so run
// integration tests one package at a time: go test -p 1 -tags integration ./...
package testenv

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/mafuky/invoice-hub/internal/platform/config"
	"github.com/mafuky/invoice-hub/internal/platform/store"
)

var (
	prepMu   sync.Mutex
	prepared = map[string]bool{}
)

// OpenMySQL opens the test database for name (invoice, provider or settlement).
// The first call in a process migrates it and empties every app table; each
// test's own rows are removed again when the test ends. It skips the test when
// the config or the <name>_test database is unavailable.
func OpenMySQL(t *testing.T, name string) *sql.DB {
	t.Helper()
	cfg, err := config.Load()
	if err != nil {
		t.Skipf("no config (copy .env.example to .env and run make tunnel): %v", err)
	}
	live := map[string]string{
		"invoice":    cfg.MySQLDSNInvoice,
		"provider":   cfg.MySQLDSNProvider,
		"settlement": cfg.MySQLDSNSettlement,
	}[name]
	if live == "" {
		t.Fatalf("testenv: unknown database %q", name)
	}
	testDB := name + "_test"
	mc, err := mysql.ParseDSN(live)
	if err != nil {
		t.Fatalf("testenv: invalid %s dsn", name)
	}
	mc.DBName = testDB
	dsn := mc.FormatDSN()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, err := store.OpenMySQL(ctx, dsn)
	if err != nil {
		t.Skipf("database %s unavailable: %v", testDB, err)
	}
	t.Cleanup(func() { db.Close() })

	prepMu.Lock()
	defer prepMu.Unlock()
	if !prepared[name] {
		if err := store.MigrateMySQL(ctx, dsn, name); err != nil {
			t.Skipf("cannot migrate %s (does the database exist and does the app user have grants?): %v", testDB, err)
		}
		if err := clear(ctx, db); err != nil {
			t.Fatalf("empty %s: %v", testDB, err)
		}
		prepared[name] = true
	}
	t.Cleanup(func() {
		cctx, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		if err := clear(cctx, db); err != nil {
			t.Logf("clean %s: %v", testDB, err)
		}
	})
	return db
}

// clear empties every table except schema_migrations.
func clear(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx,
		`SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name <> 'schema_migrations'`)
	if err != nil {
		return err
	}
	var tables []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return err
		}
		tables = append(tables, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, n := range tables {
		if _, err := db.ExecContext(ctx, "TRUNCATE TABLE `"+n+"`"); err != nil {
			return err
		}
	}
	return nil
}

// BizOrderNo returns a business order number in BIZ-YYYYMMDD-NNNN form. The
// test database starts empty, so a random date and four digits are enough.
func BizOrderNo() string {
	return fmt.Sprintf("BIZ-2026%02d%02d-%04d", 1+rand.IntN(12), 1+rand.IntN(28), rand.IntN(10000))
}
