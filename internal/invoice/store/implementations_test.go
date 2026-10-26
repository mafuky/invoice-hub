//go:build integration

package store_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/invoice/store"
	"github.com/mafuky/invoice-hub/internal/invoice/store/gormstore"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
)

// repository also covers helpers used to prepare requests in later lessons.
type repository interface {
	store.Store
	GetInvoiceByBizOrderNo(context.Context, store.DBTX, string) (domain.Invoice, error)
	InsertInvoice(context.Context, store.DBTX, store.NewInvoice) (int64, error)
	InsertCommand(context.Context, store.DBTX, int64, string, string) (int64, error)
	InsertTask(context.Context, store.DBTX, int64, int64, string) (int64, error)
	GetTask(context.Context, store.DBTX, int64) (domain.Task, error)
	GetCommand(context.Context, store.DBTX, int64) (domain.Command, error)
	ListTimedOutTasks(context.Context, time.Time, int) ([]domain.Task, error)
}

type testRepo struct {
	repository
	db *sql.DB
}

type implementation struct {
	name    string
	newRepo func(*testing.T, *sql.DB) repository
}

func implementations() []implementation {
	return []implementation{
		{"sql", func(t *testing.T, db *sql.DB) repository { return store.NewRepo(db) }},
		{"gorm", func(t *testing.T, db *sql.DB) repository {
			t.Helper()
			r, err := gormstore.NewRepo(db)
			if err != nil {
				t.Fatal(err)
			}
			return r
		}},
	}
}

func forEachStore(t *testing.T, test func(*testing.T, *testRepo)) {
	t.Helper()
	for _, impl := range implementations() {
		t.Run(impl.name, func(t *testing.T) {
			db := testenv.OpenMySQL(t, "invoice")
			test(t, &testRepo{repository: impl.newRepo(t, db), db: db})
		})
	}
}
