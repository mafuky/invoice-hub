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
// The session time zone is pinned to UTC so CURRENT_TIMESTAMP defaults agree
// with the UTC times the Go side writes.
func OpenMySQL(ctx context.Context, dsn string) (*sql.DB, error) {
	return open(ctx, dsn, false)
}

// OpenMySQLForMigrate is OpenMySQL with multiStatements on, because migration
// files hold several statements; services keep the strict default.
func OpenMySQLForMigrate(ctx context.Context, dsn string) (*sql.DB, error) {
	return open(ctx, dsn, true)
}

func open(ctx context.Context, dsn string, multi bool) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, errors.New("store: invalid mysql dsn")
	}
	cfg.MultiStatements = multi
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["time_zone"] = "'+00:00'"

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

// MigrateMySQL applies all pending migrations of the named set (invoice,
// provider or settlement) over a dedicated connection that it closes at the end.
func MigrateMySQL(ctx context.Context, dsn, name string) error {
	return runMigrate(ctx, dsn, name, "up", func(m *migrate.Migrate) error { return m.Up() })
}

// MigrateMySQLDown reverts every migration of the named set.
func MigrateMySQLDown(ctx context.Context, dsn, name string) error {
	return runMigrate(ctx, dsn, name, "down", func(m *migrate.Migrate) error { return m.Down() })
}

func runMigrate(ctx context.Context, dsn, name, dir string, step func(*migrate.Migrate) error) error {
	switch name {
	case "invoice", "provider", "settlement":
	default:
		return fmt.Errorf("store: unknown migration set %q", name)
	}
	db, err := OpenMySQLForMigrate(ctx, dsn)
	if err != nil {
		return err
	}
	src, err := iofs.New(migrations.FS, name)
	if err != nil {
		db.Close()
		return fmt.Errorf("store: migration source %s: %w", name, err)
	}
	drv, err := migmysql.WithInstance(db, &migmysql.Config{})
	if err != nil {
		db.Close()
		return fmt.Errorf("store: migration driver %s: %w", name, err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "mysql", drv)
	if err != nil {
		db.Close()
		return fmt.Errorf("store: migrate %s: %w", name, err)
	}
	// Close also closes db, which is private to this call.
	defer m.Close()
	if err := step(m); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("store: migrate %s %s: %w", name, dir, err)
	}
	return nil
}
