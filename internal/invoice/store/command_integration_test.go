//go:build integration

package store

import (
	"context"
	"testing"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
)

func TestTransitionCommand_CASLogAndRollback(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "invoice")
	r := NewRepo(db)
	id, err := r.InsertCommand(ctx, db, 1, domain.KindIssue, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		from, to domain.CommandStatus
		version  int
	}{
		{"stale", domain.CommandCreated, domain.CommandDispatched, 7},
		{"wrong from", domain.CommandDispatched, domain.CommandSucceeded, 0},
		{"illegal", domain.CommandCreated, domain.CommandSucceeded, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			ok, err := r.TransitionCommand(ctx, tx, id, tc.from, tc.to, tc.version, "test", "")
			if ok {
				t.Fatal("refused transition applied")
			}
			if tc.name != "illegal" && err != nil {
				t.Fatal(err)
			}
			c, err := r.GetCommand(ctx, tx, id)
			if err != nil {
				t.Fatal(err)
			}
			if c.Status != domain.CommandCreated || c.Version != 0 {
				t.Fatalf("changed: %+v", c)
			}
			var n int
			if err := tx.QueryRow(`SELECT COUNT(*) FROM state_log`).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Fatalf("logs=%d", n)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, commit := range []bool{false, true} {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		ok, err := r.TransitionCommand(ctx, tx, id, domain.CommandCreated, domain.CommandDispatched, 0, "provider.task.submitted", "11111111-1111-1111-1111-111111111111")
		if err != nil || !ok {
			t.Fatalf("applied=%v err=%v", ok, err)
		}
		var from, to, cause, eventID string
		if err := tx.QueryRow(`SELECT from_status, to_status, cause, event_id FROM state_log WHERE entity='command' AND entity_id=?`, id).Scan(&from, &to, &cause, &eventID); err != nil {
			t.Fatal(err)
		}
		if from != "created" || to != "dispatched" || cause != "provider.task.submitted" || eventID != "11111111-1111-1111-1111-111111111111" {
			t.Fatalf("bad log: %s %s %s %s", from, to, cause, eventID)
		}
		if commit {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
		}
		if err != nil {
			t.Fatal(err)
		}
		c, err := r.GetCommand(ctx, db, id)
		if err != nil {
			t.Fatal(err)
		}
		want, version := domain.CommandCreated, 0
		if commit {
			want, version = domain.CommandDispatched, 1
		}
		if c.Status != want || c.Version != version {
			t.Fatalf("after commit=%v: %+v", commit, c)
		}
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM state_log`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != version {
			t.Fatalf("logs=%d want=%d", n, version)
		}
	}
}
