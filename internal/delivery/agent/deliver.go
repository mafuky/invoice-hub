// Implemented in lesson 6.

// Package agent delivers issued invoices to the customer and the finance center.
package agent

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/mafuky/invoice-hub/internal/delivery/store"
	"github.com/mafuky/invoice-hub/internal/platform/event"
)

const (
	// MaxAttempts is the number of failed pushes after which delivery gives up on a channel.
	MaxAttempts = 8
	// RetryBase and RetryMax bound the wait before a failed push is tried again.
	RetryBase = 5 * time.Second
	RetryMax  = 10 * time.Minute
)

// Sender pushes one delivery to its channel and returns nil only when the
// receiver accepted it.
type Sender interface {
	Send(ctx context.Context, d store.Delivery) error
}

// DeliveryAgent turns issued invoices into deliveries and carries them out.
type DeliveryAgent struct {
	repo    *store.Repo
	senders map[string]Sender
	now     func() time.Time
	// rnd is not safe for concurrent use; one goroutine delivers at a time.
	rnd *rand.Rand
}

// NewDeliveryAgent sends each channel's deliveries through senders[channel].
func NewDeliveryAgent(repo *store.Repo, senders map[string]Sender) *DeliveryAgent {
	return &DeliveryAgent{
		repo:    repo,
		senders: senders,
		now:     time.Now,
		rnd:     rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0x64656c69)),
	}
}

// NextRetry is the wait before a failed push of an invoice is tried again,
// where attempt counts the failed pushes so far, starting at 1.
//
// For attempt >= 1, base >= 2ns, max >= base and a non-nil random source,
// cap = min(max, base*2^(attempt-1)) and the delay is in [cap/2, cap).
// The same random source state gives the same waits; jitter spreads retries
// from deliveries that failed together.
//
// If every failed delivery retries at the same instant, a customer endpoint
// that has just recovered is flooded by all of them at once and goes down
// again; if the wait is unbounded, an invoice is not delivered for days.
func NextRetry(attempt int, base, max time.Duration, rnd *rand.Rand) time.Duration {
	return 0
}

// OnInvoiceIssued handles an invoice.invoice.issued event: it queues the
// invoice for delivery to the customer and for write-back to the finance center.
//
// Invariant: each channel has exactly one delivery for an invoice version,
// however many times the event arrives, and the delivery carries what the
// receiver needs to recognise a repeated push (invoice id and version).
// The invoice-version summary document must exist before deliveries start
// (Repo.EnsureInvoiceSummary).
//
// If the event is handled twice and a second delivery is queued, the customer
// receives the same invoice twice and finance books it twice.
func (a *DeliveryAgent) OnInvoiceIssued(ctx context.Context, env event.Envelope) error {
	return fmt.Errorf("lesson 6: %w", errors.ErrUnsupported)
}

// Deliver pushes a pending delivery, or one carrying its existing claim's
// DeliveringSince, to its channel and records the outcome.
//
// Invariant: delivery is at least once: a push is never skipped because an
// earlier answer was lost. The record ends up in exactly one of delivered,
// pending with the next attempt time (after which the push error is returned),
// or gave up after MaxAttempts (8) failed pushes. The invoice-delivered event
// is announced once, only after both channels have delivered; the gave-up
// event is announced once per invoice version. Whether to announce is decided
// by Repo.RecordChannelOutcome, and the outbox row is written only when it says
// so; announcements and channel outcomes commit together. A record that another worker already took, or
// whose claim was lost (store.ErrClaimLost), is left alone with no announcement,
// and Deliver returns nil. Completed deliveries also return nil unchanged.
//
// If the customer endpoint processed a push but its answer timed out, the push
// is repeated and the customer receives the invoice twice unless the receiver
// drops the repeat by invoice id and version; if "delivered" is announced
// after only one channel succeeded, the invoice is settled although finance
// never received it; if "delivered" is never announced, it is never settled.
func (a *DeliveryAgent) Deliver(ctx context.Context, d store.Delivery) error {
	return fmt.Errorf("lesson 6: %w", errors.ErrUnsupported)
}
