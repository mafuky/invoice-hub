//go:build integration

package app

import (
	"database/sql"
	"testing"

	"github.com/mafuky/invoice-hub/internal/invoice/store"
	"github.com/mafuky/invoice-hub/internal/invoice/store/gormstore"
	"github.com/mafuky/invoice-hub/internal/platform/consume"
	"github.com/mafuky/invoice-hub/internal/platform/outbox"
)

func forEachPersistence(t *testing.T, fn func(*testing.T, string)) {
	t.Helper()
	for _, impl := range []string{"sql", "gorm"} {
		t.Run(impl, func(t *testing.T) { fn(t, impl) })
	}
}

func newRepo(t *testing.T, db *sql.DB, impl string) store.Store {
	t.Helper()
	if impl == "sql" {
		return store.NewRepo(db)
	}
	r, err := gormstore.NewRepo(db)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func persistenceOption(impl string) Option {
	return WithPersistence(consume.ForImplementation(impl), outbox.ForImplementation(impl))
}
