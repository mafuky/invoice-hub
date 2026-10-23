//go:build integration

package mq

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/mafuky/invoice-hub/internal/platform/config"
	"github.com/mafuky/invoice-hub/internal/platform/event"
)

func dialOrSkip(t *testing.T) *amqp.Connection {
	t.Helper()
	cfg, err := config.Load()
	if err != nil {
		t.Skipf("no config (copy .env.example to .env and run make tunnel): %v", err)
	}
	uri, err := amqp.ParseURI(cfg.AMQPURL)
	if err != nil {
		t.Skipf("bad amqp url in config: %v", err)
	}
	c, err := net.DialTimeout("tcp", net.JoinHostPort(uri.Host, fmt.Sprint(uri.Port)), 2*time.Second)
	if err != nil {
		t.Skipf("amqp port closed (run make tunnel): %v", err)
	}
	c.Close()
	conn, err := amqp.Dial(cfg.AMQPURL)
	if err != nil {
		t.Skipf("amqp unavailable: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// setup declares an isolated topology under a per-run prefix and removes it afterwards.
func setup(t *testing.T, conn *amqp.Connection) (prefix string, ch *amqp.Channel) {
	t.Helper()
	prefix = fmt.Sprintf("t%08x.", rand.Uint32())
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if err := DeclareWithPrefix(ch, prefix); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cc, err := conn.Channel()
		if err != nil {
			return
		}
		defer cc.Close()
		for _, b := range bindings {
			q := prefix + b.queue
			for _, name := range []string{q, q + parkingSuffix, q + ".retry.5s", q + ".retry.30s", q + ".retry.5m"} {
				_, _ = cc.QueueDelete(name, false, false, false)
			}
		}
		_ = cc.ExchangeDelete(prefix+exchangeName, false, false)
	})
	return prefix, ch
}

func newEnvelope(t *testing.T) event.Envelope {
	t.Helper()
	env, err := event.New(InvoiceTaskCreated, "1", 1, map[string]string{"biz_order_no": "BIZ-20261023-0001", "amount": "1999.99"}, "tr-mq-1")
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func TestDeclare_Idempotent(t *testing.T) {
	conn := dialOrSkip(t)
	prefix, ch := setup(t, conn)
	if err := DeclareWithPrefix(ch, prefix); err != nil {
		t.Fatalf("second declare: %v", err)
	}
}

func TestPublish_WaitsForConfirm(t *testing.T) {
	conn := dialOrSkip(t)
	prefix, ch := setup(t, conn)
	pub, err := NewPublisherWithPrefix(conn, prefix)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	env := newEnvelope(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pub.Publish(ctx, InvoiceTaskCreated, env); err != nil {
		t.Fatal(err)
	}
	// Confirmed means the broker holds it, so a single basic.get must find it.
	msg, ok, err := ch.Get(prefix+QueueProvider, true)
	if err != nil || !ok {
		t.Fatalf("message not in q.provider: ok=%v err=%v", ok, err)
	}
	if msg.MessageId != env.EventID {
		t.Fatalf("message id %q, want %q", msg.MessageId, env.EventID)
	}
	if msg.DeliveryMode != amqp.Persistent {
		t.Fatalf("delivery mode %d, want persistent", msg.DeliveryMode)
	}
}

func TestPublish_UnroutableKeyIsAnError(t *testing.T) {
	conn := dialOrSkip(t)
	prefix, _ := setup(t, conn)
	pub, err := NewPublisherWithPrefix(conn, prefix)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pub.Publish(ctx, "nobody.listens.here", newEnvelope(t)); err == nil {
		t.Fatal("unroutable event reported as published")
	}
}

func TestConsume_HandlerErrorGoesToRetryThenBack(t *testing.T) {
	conn := dialOrSkip(t)
	prefix, _ := setup(t, conn)

	orig := retryTierImpl
	retryTierImpl = func(attempt int) (string, bool) {
		if attempt == 1 {
			return ".retry.5s", true
		}
		return "", false
	}
	defer func() { retryTierImpl = orig }()

	pub, err := NewPublisherWithPrefix(conn, prefix)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	calls := make(chan time.Time, 4)
	traces := make(chan string, 4)
	h := func(ctx context.Context, env event.Envelope) error {
		calls <- time.Now()
		traces <- env.TraceID
		if len(calls) == 1 {
			return fmt.Errorf("customer endpoint timeout for %s", env.AggregateID)
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Consume(ctx, conn, prefix+QueueProvider, 1, h) }()

	pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
	defer pcancel()
	if err := pub.Publish(pctx, InvoiceTaskCreated, newEnvelope(t)); err != nil {
		t.Fatal(err)
	}

	var first, second time.Time
	select {
	case first = <-calls:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never called")
	}
	select {
	case second = <-calls:
	case <-time.After(15 * time.Second):
		t.Fatal("message did not come back from retry.5s")
	}
	if gap := second.Sub(first); gap < 4*time.Second {
		t.Fatalf("redelivered after %v, want about 5s", gap)
	}
	if tr := <-traces; tr != "tr-mq-1" {
		t.Fatalf("trace id %q", tr)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("consume stopped with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("consume did not stop after cancel")
	}
}

func TestConsume_ExhaustedTiersAndPoisonGoToParking(t *testing.T) {
	conn := dialOrSkip(t)
	prefix, ch := setup(t, conn)

	// The default stub has no tiers, so a failing event is parked straight away.
	pub, err := NewPublisherWithPrefix(conn, prefix)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Consume(ctx, conn, prefix+QueueProvider, 1, func(context.Context, event.Envelope) error {
		return fmt.Errorf("supplier rejected BIZ-20261023-0002")
	})
	if err := pub.Publish(ctx, InvoiceTaskCreated, newEnvelope(t)); err != nil {
		t.Fatal(err)
	}
	if err := ch.PublishWithContext(ctx, "", prefix+QueueProvider, false, false, amqp.Publishing{Body: []byte("not json")}); err != nil {
		t.Fatal(err)
	}

	parked := 0
	deadline := time.Now().Add(10 * time.Second)
	for parked < 2 && time.Now().Before(deadline) {
		if _, ok, _ := ch.Get(prefix+QueueProvider+parkingSuffix, true); ok {
			parked++
			continue
		}
		time.Sleep(200 * time.Millisecond)
	}
	if parked != 2 {
		t.Fatalf("parked %d messages, want 2", parked)
	}
}
