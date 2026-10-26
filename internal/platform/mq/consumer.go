package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/logx"
)

// Handler processes one event. A nil return acks it; an error (or a panic)
// sends it to the next retry tier or to the parking queue.
type Handler func(ctx context.Context, env event.Envelope) error

// relayState is the confirm-mode channel that moves failed messages to retry or parking queues.
type relayState struct {
	ch      *amqp.Channel
	returns <-chan amqp.Return
	closed  <-chan *amqp.Error
}

// Consume reads queue until ctx is cancelled. A handler error sends the message
// to the next retry tier, or to the parking queue once the tiers run out; an
// undecodable message is parked at once. The original is acked only after the
// retry or parking copy has been confirmed, so a crash in between redelivers it.
// It returns an error if the topology is missing, the relay channel dies, or the
// broker cannot route a retry copy; the service should then restart.
func Consume(ctx context.Context, conn *amqp.Connection, queue string, prefetch int, h Handler) error {
	if prefetch <= 0 {
		return fmt.Errorf("mq: prefetch must be positive, got %d", prefetch)
	}
	if err := checkTopology(conn, queue); err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("mq: open consumer channel: %w", err)
	}
	defer ch.Close()
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("mq: set prefetch: %w", err)
	}
	rch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("mq: open relay channel: %w", err)
	}
	defer rch.Close()
	if err := rch.Confirm(false); err != nil {
		return fmt.Errorf("mq: enable confirms on relay channel: %w", err)
	}
	relay := &relayState{
		ch:      rch,
		returns: rch.NotifyReturn(make(chan amqp.Return, 16)),
		closed:  rch.NotifyClose(make(chan *amqp.Error, 1)),
	}

	tag := "c-" + queue
	deliveries, err := ch.Consume(queue, tag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("mq: consume %s: %w", queue, err)
	}
	log := slog.Default().With("queue", queue)

	for {
		if ctx.Err() != nil {
			return stop(ch, tag, queue)
		}
		select {
		case <-ctx.Done():
			return stop(ch, tag, queue)
		case cerr := <-relay.closed:
			return fmt.Errorf("mq: relay channel for %s closed: %v", queue, cerr)
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("mq: deliveries for %s closed", queue)
			}
			if err := handle(ctx, log, relay, queue, d, h); err != nil {
				return err
			}
		}
	}
}

// stop cancels the consumer; any handler already finished because the loop is serial.
func stop(ch *amqp.Channel, tag, queue string) error {
	if err := ch.Cancel(tag, false); err != nil {
		return fmt.Errorf("mq: cancel consumer %s: %w", queue, err)
	}
	return nil
}

// checkTopology fails fast when the main, retry or parking queues are missing,
// since a retry copy sent to a missing queue would otherwise be lost.
func checkTopology(conn *amqp.Connection, queue string) error {
	names := []string{queue, queue + parkingSuffix}
	for _, t := range tiers {
		names = append(names, queue+t.suffix)
	}
	for _, name := range names {
		// A failed passive declare closes its channel, so each check gets its own.
		ch, err := conn.Channel()
		if err != nil {
			return fmt.Errorf("mq: open check channel: %w", err)
		}
		_, err = ch.QueueDeclarePassive(name, true, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("mq: queue %s is missing, run Declare first: %w", name, err)
		}
		ch.Close()
	}
	return nil
}

// handle returns an error only when the relay is unusable and Consume must stop.
func handle(ctx context.Context, log *slog.Logger, relay *relayState, queue string, d amqp.Delivery, h Handler) error {
	var env event.Envelope
	if err := json.Unmarshal(d.Body, &env); err != nil {
		log.ErrorContext(ctx, "undecodable message, parking", "message_id", d.MessageId, "err", err)
		return settle(ctx, log, relay, d, queue+parkingSuffix, attemptOf(d))
	}

	// The handler finishes even if shutdown starts while it runs.
	hctx := logx.WithTrace(context.WithoutCancel(ctx), env.TraceID)
	err := runHandler(hctx, log, h, env)
	if err == nil {
		if aerr := d.Ack(false); aerr != nil {
			log.ErrorContext(hctx, "ack failed", "event_id", env.EventID, "err", aerr)
		}
		return nil
	}

	attempt := attemptOf(d) + 1
	target := queue + parkingSuffix
	if suffix, ok := retryTierImpl(attempt); ok {
		target = queue + suffix
	}
	log.WarnContext(hctx, "handler failed", "event_id", env.EventID, "type", env.Type, "attempt", attempt, "next", target, "err", err)
	return settle(hctx, log, relay, d, target, attempt)
}

// runHandler turns a handler panic into an error so the message takes the retry path
// instead of being redelivered unchanged forever.
func runHandler(ctx context.Context, log *slog.Logger, h Handler, env event.Envelope) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.ErrorContext(ctx, "handler panicked", "event_id", env.EventID, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			err = fmt.Errorf("mq: handler panic: %v", r)
		}
	}()
	return h(ctx, env)
}

// settle copies the message to target, waits for the broker confirm, then acks the original.
// It returns an error when the relay channel is dead or the broker returned the copy as unroutable.
func settle(ctx context.Context, log *slog.Logger, relay *relayState, d amqp.Delivery, target string, attempt int) error {
	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers[headerAttempt] = int32(attempt)

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultConfirmTimeout)
	defer cancel()
	drainReturns(relay.returns)
	err := republish(ctx, relay.ch, target, amqp.Publishing{
		ContentType:  d.ContentType,
		DeliveryMode: amqp.Persistent,
		MessageId:    d.MessageId,
		Timestamp:    d.Timestamp,
		Type:         d.Type,
		Headers:      headers,
		Body:         d.Body,
	})
	if err == nil {
		if r, ok := findReturn(relay.returns, d.MessageId); ok {
			_ = d.Nack(false, true)
			return fmt.Errorf("mq: %s returned unroutable (%d %s), original requeued", target, r.ReplyCode, r.ReplyText)
		}
		if relay.ch.IsClosed() {
			_ = d.Nack(false, true)
			return fmt.Errorf("mq: relay channel closed while moving %s: %w", d.MessageId, amqp.ErrClosed)
		}
		if aerr := d.Ack(false); aerr != nil {
			log.ErrorContext(ctx, "ack after move failed", "message_id", d.MessageId, "err", aerr)
		}
		return nil
	}
	log.ErrorContext(ctx, "could not move message, requeueing", "message_id", d.MessageId, "target", target, "err", err)
	_ = d.Nack(false, true)
	if relay.ch.IsClosed() || errors.Is(err, amqp.ErrClosed) {
		return fmt.Errorf("mq: relay channel closed while moving %s: %w", d.MessageId, err)
	}
	return nil
}

func republish(ctx context.Context, ch *amqp.Channel, queue string, msg amqp.Publishing) error {
	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, true, false, msg)
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
	n := 0
	switch v := d.Headers[headerAttempt].(type) {
	case int32:
		n = int(v)
	case int64:
		n = int(v)
	case int:
		n = v
	case int16:
		n = int(v)
	case int8:
		n = int(v)
	case uint8:
		n = int(v)
	case uint16:
		n = int(v)
	case uint32:
		n = int(v)
	case string:
		n, _ = strconv.Atoi(v)
	}
	return max(n, 0)
}
