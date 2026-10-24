//go:build lesson && integration

package outbox

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

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

func drain(ctx context.Context, r *Relay) error {
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
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "invoice")

	const total = 100
	ids := make([]string, 0, total)
	for i := range total {
		env, err := event.New("invoice.task.created", fmt.Sprint(i), 0, map[string]int{"task_id": i}, "trace-relay")
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
		ids = append(ids, env.EventID)
	}

	pub := &countingPublisher{seen: map[string]int{}}
	relays := []*Relay{NewRelay(db, pub, "a"), NewRelay(db, pub, "b")}
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
	args := make([]any, total)
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
}

type failingPublisher struct{}

func (failingPublisher) Publish(context.Context, string, event.Envelope) error {
	return fmt.Errorf("broker nacked")
}

func TestRelay_BrokerNack_RowStaysNewWithLaterAttempt(t *testing.T) {
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

	if _, err := NewRelay(db, failingPublisher{}, "nack").RunOnce(ctx); err != nil {
		t.Logf("RunOnce reported: %v", err)
	}

	var status string
	var attempts int
	var later bool
	err = db.QueryRowContext(ctx, `SELECT status, attempts, next_attempt_at > NOW(3) FROM outbox WHERE event_id = ?`, env.EventID).Scan(&status, &attempts, &later)
	if err != nil {
		t.Fatal(err)
	}
	if status != "new" || attempts != 1 || !later {
		t.Fatalf("after a nack: status=%s attempts=%d next_attempt_at_in_future=%v", status, attempts, later)
	}
}
