// Implemented in lesson 3.

// Package consume runs event handlers at most once per event_id.
package consume

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mafuky/invoice-hub/internal/platform/event"
)

// Handle runs fn for env inside one database transaction that also records
// env.EventID in processed_events for the named consumer. It reports
// duplicate=true, without running fn, when the event was already processed.
//
// Invariant: fn's effects and the processed_events row commit together or not
// at all; an event whose fn failed stays unprocessed so it can be retried,
// and an event that already succeeded is never applied again.
//
// A redelivered "delivered" event applied twice puts the same invoice into
// two settlement items and the customer is paid for twice.
func Handle(ctx context.Context, db *sql.DB, consumer string, env event.Envelope, fn func(tx *sql.Tx) error) (duplicate bool, err error) {
	return false, fmt.Errorf("lesson 3: %w", errors.ErrUnsupported)
}
