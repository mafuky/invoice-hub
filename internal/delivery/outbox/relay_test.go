package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/mafuky/invoice-hub/internal/platform/event"
	platform "github.com/mafuky/invoice-hub/internal/platform/outbox"
)

type fakeRows struct {
	mu       sync.Mutex
	queue    []row
	sent     []bson.ObjectID
	failed   map[bson.ObjectID]int
	next     map[bson.ObjectID]time.Time
	released []bson.ObjectID
}

func newFakeRows(rs ...row) *fakeRows {
	return &fakeRows{queue: rs, failed: map[bson.ObjectID]int{}, next: map[bson.ObjectID]time.Time{}}
}

func (f *fakeRows) claim(_ context.Context, _ time.Time, _ time.Duration) (row, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.queue) == 0 {
		return row{}, false, nil
	}
	r := f.queue[0]
	f.queue = f.queue[1:]
	return r, true, nil
}

func (f *fakeRows) markSent(_ context.Context, id bson.ObjectID, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, id)
	return nil
}

func (f *fakeRows) markFailed(_ context.Context, id bson.ObjectID, attempts int, next time.Time, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed[id] = attempts
	f.next[id] = next
	return nil
}

func (f *fakeRows) release(_ context.Context, id bson.ObjectID, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.released = append(f.released, id)
	return nil
}

type fakePub struct {
	err  error
	keys []string
}

func (p *fakePub) Publish(_ context.Context, key string, _ event.Envelope) error {
	if p.err != nil {
		return p.err
	}
	p.keys = append(p.keys, key)
	return nil
}

var _ platform.Publisher = (*fakePub)(nil)

func outboxRow(t *testing.T, key string, invoiceID int64, attempts int) row {
	t.Helper()
	env, err := event.New(key, fmt.Sprint(invoiceID), 1, event.DeliveryDone{InvoiceID: invoiceID, InvoiceVersion: 1}, "trace-outbox")
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	return row{ID: bson.NewObjectID(), EventID: env.EventID, RoutingKey: key, Body: string(body), Status: statusPending, Attempts: attempts}
}

func TestRunOnce_ConfirmedRowsAreMarkedSent(t *testing.T) {
	a := outboxRow(t, "delivery.invoice.delivered", 101, 0)
	b := outboxRow(t, "delivery.invoice.gave_up", 102, 0)
	rows, pub := newFakeRows(a, b), &fakePub{}

	n, err := newRelay(rows, pub, "delivery").RunOnce(context.Background())
	if err != nil || n != 2 {
		t.Fatalf("RunOnce = %d, %v; want 2, nil", n, err)
	}
	if len(rows.sent) != 2 || rows.sent[0] != a.ID || rows.sent[1] != b.ID {
		t.Fatalf("sent = %v, want both rows in order", rows.sent)
	}
	if len(pub.keys) != 2 || pub.keys[0] != "delivery.invoice.delivered" || pub.keys[1] != "delivery.invoice.gave_up" {
		t.Fatalf("routing keys = %v", pub.keys)
	}
}

func TestRunOnce_PublishFailureBacksOffAndKeepsRow(t *testing.T) {
	r := outboxRow(t, "delivery.invoice.delivered", 103, 2)
	rows, pub := newFakeRows(r), &fakePub{err: errors.New("broker nacked")}
	fixed := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	relay := newRelay(rows, pub, "delivery")
	relay.now = func() time.Time { return fixed }

	n, err := relay.RunOnce(context.Background())
	if err == nil || n != 0 {
		t.Fatalf("RunOnce = %d, %v; want 0 and an error", n, err)
	}
	if len(rows.sent) != 0 {
		t.Fatalf("a row that failed to publish was marked sent: %v", rows.sent)
	}
	if rows.failed[r.ID] != 3 {
		t.Fatalf("attempts = %d, want 3", rows.failed[r.ID])
	}
	if want := fixed.Add(platform.Backoff(3)); !rows.next[r.ID].Equal(want) {
		t.Fatalf("next attempt = %v, want %v", rows.next[r.ID], want)
	}
}

func TestRunOnce_PublisherClosedIsReturnedAndNotCountedAsAttempt(t *testing.T) {
	r := outboxRow(t, "delivery.invoice.delivered", 104, 0)
	rows := newFakeRows(r)
	pub := &fakePub{err: fmt.Errorf("mq: publish: %w", amqp.ErrClosed)}

	_, err := newRelay(rows, pub, "delivery").RunOnce(context.Background())
	if !errors.Is(err, amqp.ErrClosed) {
		t.Fatalf("err = %v, want one wrapping amqp.ErrClosed", err)
	}
	if len(rows.failed) != 0 || len(rows.sent) != 0 {
		t.Fatalf("row was marked: failed=%v sent=%v", rows.failed, rows.sent)
	}
	if len(rows.released) != 1 || rows.released[0] != r.ID {
		t.Fatalf("lease was not released: %v", rows.released)
	}
}

func TestRun_ReturnsWhenPublisherClosed(t *testing.T) {
	rows := newFakeRows(outboxRow(t, "delivery.invoice.delivered", 105, 0))
	pub := &fakePub{err: amqp.ErrClosed}
	done := make(chan error, 1)
	go func() { done <- newRelay(rows, pub, "delivery").Run(context.Background()) }()

	select {
	case err := <-done:
		if !errors.Is(err, amqp.ErrClosed) {
			t.Fatalf("Run = %v, want an error wrapping amqp.ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the publisher closed")
	}
}

func TestRun_StopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- newRelay(newFakeRows(), &fakePub{}, "delivery").Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil on cancel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run ignored cancellation")
	}
}
