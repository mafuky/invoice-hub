// Package store opens the MySQL and MongoDB backends and owns their schema setup.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/golang-migrate/migrate/v4"
	migmysql "github.com/golang-migrate/migrate/v4/database/mysql"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/mafuky/invoice-hub/internal/platform/dial"
	"github.com/mafuky/invoice-hub/migrations"
)

const tunnelHint = "run `make tunnel`"

// OpenMySQL checks that the server is reachable, then opens and pings a pool.
// Multi-statement mode is switched on because the migration files hold several statements.
func OpenMySQL(ctx context.Context, dsn string) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, errors.New("store: invalid mysql dsn")
	}
	cfg.MultiStatements = true
	cfg.ParseTime = true
	cfg.Loc = time.UTC

	if err := dial.Check(cfg.Addr, 5*time.Second, tunnelHint); err != nil {
		return nil, fmt.Errorf("store: mysql: %w", err)
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("store: open mysql: %w", err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: ping mysql %s: %w", cfg.Addr, err)
	}
	return db, nil
}

func newMigrate(db *sql.DB, name string) (*migrate.Migrate, error) {
	switch name {
	case "invoice", "provider", "settlement":
	default:
		return nil, fmt.Errorf("store: unknown migration set %q", name)
	}
	src, err := iofs.New(migrations.FS, name)
	if err != nil {
		return nil, fmt.Errorf("store: migration source %s: %w", name, err)
	}
	drv, err := migmysql.WithInstance(db, &migmysql.Config{})
	if err != nil {
		return nil, fmt.Errorf("store: migration driver %s: %w", name, err)
	}
	return migrate.NewWithInstance("iofs", src, "mysql", drv)
}

// MigrateMySQL applies all pending migrations of the named set
// (invoice, provider or settlement). The db must allow multiple statements,
// which OpenMySQL enables. The db is left open.
func MigrateMySQL(db *sql.DB, name string) error {
	m, err := newMigrate(db, name)
	if err != nil {
		return err
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("store: migrate %s up: %w", name, err)
	}
	return nil
}

// MigrateMySQLDown reverts every migration of the named set.
func MigrateMySQLDown(db *sql.DB, name string) error {
	m, err := newMigrate(db, name)
	if err != nil {
		return err
	}
	if err := m.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("store: migrate %s down: %w", name, err)
	}
	return nil
}
