//go:build integration

package gormx_test

import (
	"context"
	"testing"

	"github.com/mafuky/invoice-hub/internal/platform/gormx"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
)

func TestSession_CallerRollbackRevertsWrite(t *testing.T) {
	db := testenv.OpenMySQL(t, "invoice")
	sessions, err := gormx.New(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	session := sessions.Session(ctx, tx)
	if err := session.Table("commands").Create(map[string]any{"invoice_id": 1, "kind": "ISSUE", "status": "created"}).Error; err != nil {
		t.Fatal(err)
	}
	var n int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM commands").Scan(&n); err != nil || n != 1 {
		t.Fatalf("inside tx: n=%d err=%v", n, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM commands").Scan(&n); err != nil || n != 0 {
		t.Fatalf("after rollback: n=%d err=%v", n, err)
	}
	if err := sessions.Session(ctx, db).Table("commands").Create(map[string]any{"invoice_id": 2, "kind": "ISSUE", "status": "created"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM commands").Scan(&n); err != nil || n != 1 {
		t.Fatalf("db session: n=%d err=%v", n, err)
	}
}
