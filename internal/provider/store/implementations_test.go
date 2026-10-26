//go:build integration

package store_test

import (
	"database/sql"
	"testing"

	"github.com/mafuky/invoice-hub/internal/platform/testenv"
	"github.com/mafuky/invoice-hub/internal/provider/store"
	"github.com/mafuky/invoice-hub/internal/provider/store/gormstore"
)

func forEachStore(t *testing.T, test func(*testing.T, *sql.DB, store.Store)) {
	t.Helper()
	for _, impl := range []string{"sql", "gorm"} {
		t.Run(impl, func(t *testing.T) {
			db := testenv.OpenMySQL(t, "provider")
			var repo store.Store = store.NewRepo(db)
			if impl == "gorm" {
				var err error
				repo, err = gormstore.NewRepo(db)
				if err != nil {
					t.Fatal(err)
				}
			}
			test(t, db, repo)
		})
	}
}
