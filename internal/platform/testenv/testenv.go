//go:build integration

// Package testenv gives integration tests a migrated MySQL database from the local config.
package testenv

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/mafuky/invoice-hub/internal/platform/config"
	"github.com/mafuky/invoice-hub/internal/platform/store"
)

// OpenMySQL opens the named database (invoice, provider or settlement) with its
// schema applied, and skips the test when the config or the server is missing.
func OpenMySQL(t *testing.T, name string) *sql.DB {
	t.Helper()
	cfg, err := config.Load()
	if err != nil {
		t.Skipf("no config (copy .env.example to .env and run make tunnel): %v", err)
	}
	dsn := map[string]string{
		"invoice":    cfg.MySQLDSNInvoice,
		"provider":   cfg.MySQLDSNProvider,
		"settlement": cfg.MySQLDSNSettlement,
	}[name]
	if dsn == "" {
		t.Fatalf("testenv: unknown database %q", name)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, err := store.OpenMySQL(ctx, dsn)
	if err != nil {
		t.Skipf("mysql unavailable: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := store.MigrateMySQL(ctx, dsn, name); err != nil {
		t.Fatalf("migrate %s: %v", name, err)
	}
	return db
}

// BizOrderNo returns a business order number that no earlier run has used.
func BizOrderNo() string {
	return fmt.Sprintf("BIZ-20261023-%04d-%08x", rand.IntN(10000), rand.Uint32())
}
