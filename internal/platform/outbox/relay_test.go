//go:build lesson && integration

package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
)

type countingPublisher struct {
	mu   sync.Mutex
	seen map[string]int
}

func (p *countingPublisher) Publish(_ context.Context, _ string, env event.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen[env.EventID]++
	return nil
}

func drain(ctx context.Context, r relayRunner) error {
	for {
		n, err := r.RunOnce(ctx)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

func TestRelay_SkipLockedTwoRelaysNoDoubleSend(t *testing.T) {
	forEachImplementation(t, func(t *testing.T, newRelay func(*sql.DB, Publisher, string) relayRunner) {
		ctx := context.Background()
		db := testenv.OpenMySQL(t, "invoice")

		const total = 100
		ids := make([]string, 0, total)
		values := make([]string, 0, total)
		args := make([]any, 0, total*3)
		for i := range total {
			env, err := event.New("invoice.task.created", fmt.Sprint(i), 0, map[string]int{"task_id": i}, "trace-relay")
			if err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(env)
			if err != nil {
				t.Fatal(err)
			}
			values = append(values, "(?, ?, ?)")
			args = append(args, env.EventID, "invoice.task.created", body)
			ids = append(ids, env.EventID)
		}
		if _, err := db.ExecContext(ctx, "INSERT INTO outbox (event_id, routing_key, body) VALUES "+strings.Join(values, ","), args...); err != nil {
			t.Fatal(err)
		}

		pub := &countingPublisher{seen: map[string]int{}}
		relays := []relayRunner{newRelay(db, pub, "a"), newRelay(db, pub, "b")}
		errs := make([]error, len(relays))
		var wg sync.WaitGroup
		for i, r := range relays {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = drain(ctx, r)
			}()
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("relay %d: %v", i, err)
			}
		}

		for id, n := range pub.seen {
			if n != 1 {
				t.Errorf("event %s published %d times", id, n)
			}
		}
		for _, id := range ids {
			if pub.seen[id] != 1 {
				t.Errorf("event %s published %d times, want 1", id, pub.seen[id])
			}
		}

		placeholders := strings.TrimSuffix(strings.Repeat("?,", total), ",")
		args = make([]any, total)
		for i, id := range ids {
			args[i] = id
		}
		var sent int
		err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox WHERE status = 'sent' AND event_id IN (`+placeholders+`)`, args...).Scan(&sent)
		if err != nil {
			t.Fatal(err)
		}
		if sent != total {
			t.Fatalf("%d of %d outbox rows marked sent", sent, total)
		}
	})
}

type failingPublisher struct{ attemptedAt time.Time }

func (p *failingPublisher) Publish(context.Context, string, event.Envelope) error {
	p.attemptedAt = time.Now().UTC()
	return fmt.Errorf("broker nacked")
}

func TestRelay_BrokerNack_RowStaysNewWithLaterAttempt(t *testing.T) {
	forEachImplementation(t, func(t *testing.T, newRelay func(*sql.DB, Publisher, string) relayRunner) {
		ctx := context.Background()
		db := testenv.OpenMySQL(t, "invoice")

		env, err := event.New("invoice.task.created", "1", 0, map[string]int{"task_id": 1}, "trace-relay")
		if err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := Insert(ctx, tx, "invoice.task.created", env); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		pub := &failingPublisher{}
		if _, err := newRelay(db, pub, "nack").RunOnce(ctx); err != nil {
			t.Logf("RunOnce reported: %v", err)
		}

		var status string
		var attempts int
		var next time.Time
		err = db.QueryRowContext(ctx, `SELECT status, attempts, next_attempt_at FROM outbox WHERE event_id = ?`, env.EventID).Scan(&status, &attempts, &next)
		if err != nil {
			t.Fatal(err)
		}
		if status != "new" || attempts != 1 || pub.attemptedAt.IsZero() || next.Before(pub.attemptedAt.Add(Backoff(0)-time.Millisecond)) {
			t.Fatalf("after a nack: status=%s attempts=%d next_attempt_at=%v attempted_at=%v", status, attempts, next, pub.attemptedAt)
		}
	})
}

type closedPublisher struct{}

func (closedPublisher) Publish(context.Context, string, event.Envelope) error {
	return fmt.Errorf("mq: publish invoice.task.created: %w", amqp.ErrClosed)
}

func TestRelay_PublisherChannelClosed_AbortsPassWithoutMarkingFailure(t *testing.T) {
	forEachImplementation(t, func(t *testing.T, newRelay func(*sql.DB, Publisher, string) relayRunner) {
		ctx := context.Background()
		db := testenv.OpenMySQL(t, "invoice")

		env, err := event.New("invoice.task.created", "1", 0, map[string]int{"task_id": 1}, "trace-relay")
		if err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := Insert(ctx, tx, "invoice.task.created", env); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		_, err = newRelay(db, closedPublisher{}, "closed").RunOnce(ctx)
		if !errors.Is(err, amqp.ErrClosed) {
			t.Fatalf("RunOnce error = %v, want one wrapping amqp.ErrClosed", err)
		}
		var status string
		var attempts int
		if err := db.QueryRowContext(ctx, `SELECT status, attempts FROM outbox WHERE event_id = ?`, env.EventID).Scan(&status, &attempts); err != nil {
			t.Fatal(err)
		}
		if status != "new" || attempts != 0 {
			t.Fatalf("after a closed channel: status=%s attempts=%d, want new and 0", status, attempts)
		}
	})
}

type relayRunner interface {
	RunOnce(context.Context) (int, error)
}

func forEachImplementation(t *testing.T, test func(*testing.T, func(*sql.DB, Publisher, string) relayRunner)) {
	t.Helper()
	for _, impl := range []struct {
		name string
		run  func(*sql.DB, Publisher, string) relayRunner
	}{{"sql", func(db *sql.DB, pub Publisher, name string) relayRunner {
		return NewRelayForImplementation(db, pub, name, "sql")
	}}, {"gorm", func(db *sql.DB, pub Publisher, name string) relayRunner {
		return NewRelayForImplementation(db, pub, name, "gorm")
	}}} {
		t.Run(impl.name, func(t *testing.T) { test(t, impl.run) })
	}
}
