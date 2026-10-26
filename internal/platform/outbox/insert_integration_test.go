//go:build integration

package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
)

func TestInsert_CommitsAndRollsBackWithCaller(t *testing.T) {
	for _, impl := range []struct {
		name   string
		insert func(context.Context, *sql.Tx, string, event.Envelope) error
	}{{"sql", ForImplementation("sql")}, {"gorm", ForImplementation("gorm")}} {
		t.Run(impl.name, func(t *testing.T) {
			db := testenv.OpenMySQL(t, "provider")
			ctx := context.Background()
			env, err := event.New("provider.task.submitted", "1", 0, map[string]string{"amount": "100.10"}, "trace-outbox")
			if err != nil {
				t.Fatal(err)
			}
			for _, commit := range []bool{false, true} {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := impl.insert(ctx, tx, env.Type, env); err != nil {
					_ = tx.Rollback()
					t.Fatal(err)
				}
				if commit {
					err = tx.Commit()
				} else {
					err = tx.Rollback()
				}
				if err != nil {
					t.Fatal(err)
				}
				var n int
				if err := db.QueryRow("SELECT COUNT(*) FROM outbox WHERE event_id = ?", env.EventID).Scan(&n); err != nil {
					t.Fatal(err)
				}
				want := 0
				if commit {
					want = 1
				}
				if n != want {
					t.Fatalf("commit=%v rows=%d want=%d", commit, n, want)
				}
			}
			var body, status string
			var attempts int
			var due bool
			if err := db.QueryRow("SELECT body, status, attempts, next_attempt_at <= NOW(3) FROM outbox WHERE event_id = ?", env.EventID).Scan(&body, &status, &attempts, &due); err != nil {
				t.Fatal(err)
			}
			var got event.Envelope
			if err := json.Unmarshal([]byte(body), &got); err != nil {
				t.Fatal(err)
			}
			if got.EventID != env.EventID || got.Type != env.Type || got.TraceID != env.TraceID || status != "new" || attempts != 0 || !due {
				t.Fatalf("outbox defaults or envelope changed: %+v status=%s attempts=%d due=%v", got, status, attempts, due)
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if err := impl.insert(ctx, tx, env.Type, env); err == nil {
				t.Fatal("duplicate event accepted")
			}
		})
	}
}
