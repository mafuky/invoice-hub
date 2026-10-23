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
type Publisher struct {
	mu       sync.Mutex
	ch       *amqp.Channel
	returns  chan amqp.Return
	exchange string
}

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
	select {
	case r := <-p.returns:
		return fmt.Errorf("mq: %s returned unroutable: %d %s", routingKey, r.ReplyCode, r.ReplyText)
	default:
	}
	return nil
}

var errNoConfirm = errors.New("mq: broker did not confirm")
