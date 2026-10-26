package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/mafuky/invoice-hub/internal/platform/event"
)

const defaultConfirmTimeout = 5 * time.Second

// Publisher sends events and returns only after the broker has confirmed them.
// Once its channel closes, every Publish fails with an error wrapping
// amqp.ErrClosed; callers such as the outbox relay must treat that as fatal or
// build a new Publisher.
type Publisher struct {
	mu       sync.Mutex
	ch       *amqp.Channel
	returns  chan amqp.Return
	exchange string
}

// NewPublisher opens a confirm-mode channel that publishes to the events exchange.
func NewPublisher(conn *amqp.Connection) (*Publisher, error) {
	return NewPublisherWithPrefix(conn, "")
}

// NewPublisherWithPrefix publishes to the exchange declared by DeclareWithPrefix.
func NewPublisherWithPrefix(conn *amqp.Connection, prefix string) (*Publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("mq: open publisher channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		return nil, fmt.Errorf("mq: enable confirms: %w", err)
	}
	return &Publisher{
		ch:       ch,
		returns:  ch.NotifyReturn(make(chan amqp.Return, 16)),
		exchange: prefix + exchangeName,
	}, nil
}

func (p *Publisher) Close() error { return p.ch.Close() }

// IsClosed reports whether the underlying channel is gone.
func (p *Publisher) IsClosed() bool { return p.ch.IsClosed() }

// Publish waits for the broker ack. A nack, an unroutable event or a timeout is an error,
// so the caller keeps the outbox row and sends the event again later.
func (p *Publisher) Publish(ctx context.Context, routingKey string, env event.Envelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("mq: marshal envelope %s: %w", env.EventID, err)
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultConfirmTimeout)
		defer cancel()
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	drainReturns(p.returns)
	if p.ch.IsClosed() {
		return fmt.Errorf("mq: publish %s: %w", routingKey, amqp.ErrClosed)
	}

	dc, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, p.exchange, routingKey, true, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    env.EventID,
		Timestamp:    env.OccurredAt,
		Type:         env.Type,
		Body:         body,
	})
	if err != nil {
		return fmt.Errorf("mq: publish %s: %w", routingKey, err)
	}
	acked, err := dc.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("mq: wait confirm for %s: %w", env.EventID, err)
	}
	if !acked {
		return fmt.Errorf("mq: broker nacked %s (%s)", env.EventID, routingKey)
	}
	// A returned message is still acked by the broker, but nobody will receive it.
	if r, ok := findReturn(p.returns, env.EventID); ok {
		return fmt.Errorf("mq: %s returned unroutable: %d %s", routingKey, r.ReplyCode, r.ReplyText)
	}
	return nil
}

var errNoConfirm = errors.New("mq: broker did not confirm")
