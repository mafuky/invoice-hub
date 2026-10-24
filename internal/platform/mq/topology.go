// Package mq wires the RabbitMQ topology, a confirmed publisher and a consumer with tiered retry.
package mq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	InvoiceTaskCreated    = "invoice.task.created"
	InvoiceIssued         = "invoice.invoice.issued"
	InvoiceDelivered      = "invoice.invoice.delivered"
	ProviderTaskSubmitted = "provider.task.submitted"
	ProviderTaskSucceeded = "provider.task.succeeded"
	ProviderTaskFailed    = "provider.task.failed"
	DeliverySucceeded     = "delivery.invoice.delivered"
	DeliveryGaveUp        = "delivery.invoice.gave_up"
	SettlementItemAdded   = "settlement.item.added"
	exchangeName          = "invoicehub.events"
	retryKeyPrefix        = "retry."
	parkingSuffix         = ".parking"
	headerAttempt         = "x-attempt"
)

const (
	QueueInvoice    = "q.invoice"
	QueueProvider   = "q.provider"
	QueueDelivery   = "q.delivery"
	QueueSettlement = "q.settlement"
)

type tier struct {
	suffix string
	ttlMs  int32
}

var tiers = []tier{
	{".retry.5s", 5_000},
	{".retry.30s", 30_000},
	{".retry.5m", 300_000},
}

var bindings = []struct {
	queue string
	keys  []string
}{
	{QueueInvoice, []string{"provider.task.*", "delivery.invoice.*", SettlementItemAdded}},
	{QueueProvider, []string{InvoiceTaskCreated}},
	{QueueDelivery, []string{InvoiceIssued}},
	{QueueSettlement, []string{InvoiceDelivered}},
}

// Declare creates the exchange, queues and bindings; it is safe to call repeatedly.
func Declare(ch *amqp.Channel) error { return DeclareWithPrefix(ch, "") }

// DeclareWithPrefix prefixes the exchange and every queue name, so tests can run in isolation.
func DeclareWithPrefix(ch *amqp.Channel, prefix string) error {
	// Prefixed queues belong to tests; let the broker reap them if a run is killed.
	queueArgs := func(extra amqp.Table) amqp.Table {
		args := amqp.Table{"x-queue-type": "classic"}
		if prefix != "" {
			args["x-expires"] = int32(10 * 60 * 1000)
		}
		for k, v := range extra {
			args[k] = v
		}
		return args
	}
	ex := prefix + exchangeName
	if err := ch.ExchangeDeclare(ex, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("mq: declare exchange %s: %w", ex, err)
	}
	for _, b := range bindings {
		q := prefix + b.queue
		if _, err := ch.QueueDeclare(q, true, false, false, false, queueArgs(nil)); err != nil {
			return fmt.Errorf("mq: declare queue %s: %w", q, err)
		}
		if _, err := ch.QueueDeclare(q+parkingSuffix, true, false, false, false, queueArgs(nil)); err != nil {
			return fmt.Errorf("mq: declare queue %s: %w", q+parkingSuffix, err)
		}
		for _, key := range b.keys {
			if err := ch.QueueBind(q, key, ex, false, nil); err != nil {
				return fmt.Errorf("mq: bind %s to %s: %w", q, key, err)
			}
		}
		// Expired retry messages dead-letter to the events exchange and come back
		// only to this queue, through its own routing key.
		retryKey := retryKeyPrefix + q
		if err := ch.QueueBind(q, retryKey, ex, false, nil); err != nil {
			return fmt.Errorf("mq: bind %s to %s: %w", q, retryKey, err)
		}
		for _, t := range tiers {
			args := queueArgs(amqp.Table{
				"x-message-ttl":             t.ttlMs,
				"x-dead-letter-exchange":    ex,
				"x-dead-letter-routing-key": retryKey,
			})
			if _, err := ch.QueueDeclare(q+t.suffix, true, false, false, false, args); err != nil {
				return fmt.Errorf("mq: declare queue %s: %w", q+t.suffix, err)
			}
		}
	}
	return nil
}
