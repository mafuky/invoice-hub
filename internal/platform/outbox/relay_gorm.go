// Implemented in lesson 2.

package outbox

import (
	"context"
	"errors"
	"fmt"
)

// RunOnce sends one batch of due outbox rows and marks them sent.
//
// Invariant: a row is marked sent only after the broker confirmed it, and a
// row that failed to send is retried later and never skipped; two relays
// working on the same table must not send the same event. A publisher-closed
// error (one wrapping amqp.ErrClosed) aborts the pass and is returned, and the
// row is not marked failed; Run treats it as fatal so the process restarts.
//
// If an invoice's "task created" event is lost, the invoice stays in
// applying forever and no supplier is ever asked to issue it; if it is sent
// twice, the supplier gets two orders for one biz_order_no.
// RunOnceGorm is the GORM version of the same contract.
func (r *Relay) RunOnceGorm(ctx context.Context) (sent int, err error) {
	return 0, fmt.Errorf("lesson 2: %w", errors.ErrUnsupported)
}

// GormRelay shares polling behavior with Relay and has an independent lesson stub.
type GormRelay struct{ *Relay }

func (r *GormRelay) RunOnce(ctx context.Context) (int, error) { return r.RunOnceGorm(ctx) }
func (r *GormRelay) Run(ctx context.Context) error            { return r.run(ctx, r.RunOnceGorm) }
