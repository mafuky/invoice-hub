package agent

import (
	"context"
	"log/slog"
	"time"
)

const dueBatch = 20

// RunDue delivers the pending records whose retry time has come, one after
// another, and returns how many it attempted. A failed delivery is logged and
// does not stop the pass.
func (a *DeliveryAgent) RunDue(ctx context.Context) (int, error) {
	due, err := a.repo.ListDue(ctx, a.now().UTC(), dueBatch)
	if err != nil {
		return 0, err
	}
	for i, d := range due {
		if ctx.Err() != nil {
			return i, nil
		}
		if err := a.Deliver(ctx, d); err != nil {
			slog.WarnContext(ctx, "delivery failed", "invoice_id", d.InvoiceID, "invoice_version", d.InvoiceVersion, "channel", d.Channel, "err", err)
		}
	}
	return len(due), nil
}

// Run looks for due deliveries every interval until ctx is cancelled.
func (a *DeliveryAgent) Run(ctx context.Context, interval time.Duration) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		if _, err := a.RunDue(ctx); err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "delivery pass failed", "err", err)
		}
	}
}
