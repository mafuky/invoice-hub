package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/logx"
)

type Handler func(ctx context.Context, env event.Envelope) error

// Consume reads queue until ctx is cancelled. A handler error sends the message
// to the next retry tier, or to the parking queue once the tiers run out; an
// undecodable message is parked at once. The original is acked only after the
// retry or parking copy has been confirmed, so a crash in between redelivers it.
func Consume(ctx context.Context, conn *amqp.Connection, queue string, prefetch int, h Handler) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("mq: open consumer channel: %w", err)
	}
	defer ch.Close()
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("mq: set prefetch: %w", err)
	}
	relay, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("mq: open relay channel: %w", err)
	}
	defer relay.Close()
	if err := relay.Confirm(false); err != nil {
		return fmt.Errorf("mq: enable confirms on relay channel: %w", err)
	}

	tag := "c-" + queue
	deliveries, err := ch.Consume(queue, tag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("mq: consume %s: %w", queue, err)
	}
	log := slog.Default().With("queue", queue)

	for {
		select {
		case <-ctx.Done():
			// Stop new deliveries; the handler below runs to completion before we get here again.
			if err := ch.Cancel(tag, false); err != nil {
				return fmt.Errorf("mq: cancel consumer %s: %w", queue, err)
			}
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("mq: deliveries for %s closed", queue)
			}
			handle(ctx, log, relay, queue, d, h)
		}
	}
}

func handle(ctx context.Context, log *slog.Logger, relay *amqp.Channel, queue string, d amqp.Delivery, h Handler) {
	var env event.Envelope
	if err := json.Unmarshal(d.Body, &env); err != nil {
		log.ErrorContext(ctx, "undecodable message, parking", "message_id", d.MessageId, "err", err)
		settle(ctx, log, relay, d, queue+parkingSuffix, attemptOf(d))
		return
	}

	// The handler finishes even if shutdown starts while it runs.
	hctx := logx.WithTrace(context.WithoutCancel(ctx), env.TraceID)
	err := h(hctx, env)
	if err == nil {
		if aerr := d.Ack(false); aerr != nil {
			log.ErrorContext(hctx, "ack failed", "event_id", env.EventID, "err", aerr)
		}
		return
	}

	attempt := attemptOf(d) + 1
	target := queue + parkingSuffix
	if suffix, ok := retryTierImpl(attempt); ok {
		target = queue + suffix
	}
	log.WarnContext(hctx, "handler failed", "event_id", env.EventID, "type", env.Type, "attempt", attempt, "next", target, "err", err)
	settle(hctx, log, relay, d, target, attempt)
}

// settle copies the message to target, waits for the broker confirm, then acks the original.
func settle(ctx context.Context, log *slog.Logger, relay *amqp.Channel, d amqp.Delivery, target string, attempt int) {
	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers[headerAttempt] = int32(attempt)

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultConfirmTimeout)
	defer cancel()
	err := republish(ctx, relay, target, amqp.Publishing{
		ContentType:  d.ContentType,
		DeliveryMode: amqp.Persistent,
		MessageId:    d.MessageId,
		Timestamp:    d.Timestamp,
		Type:         d.Type,
		Headers:      headers,
		Body:         d.Body,
	})
	if err != nil {
		log.ErrorContext(ctx, "could not move message, requeueing", "message_id", d.MessageId, "target", target, "err", err)
		_ = d.Nack(false, true)
		return
	}
	if err := d.Ack(false); err != nil {
		log.ErrorContext(ctx, "ack after move failed", "message_id", d.MessageId, "err", err)
	}
}

func republish(ctx context.Context, ch *amqp.Channel, queue string, msg amqp.Publishing) error {
	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, false, false, msg)
	if err != nil {
		return err
	}
	acked, err := dc.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !acked {
		return errNoConfirm
	}
	return nil
}

func attemptOf(d amqp.Delivery) int {
	switch v := d.Headers[headerAttempt].(type) {
	case int32:
		return int(v)
	case int64:
		return int(v)
	case int:
		return v
	case int16:
		return int(v)
	case int8:
		return int(v)
	case uint8:
		return int(v)
	}
	return 0
}
