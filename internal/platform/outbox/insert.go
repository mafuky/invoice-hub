// Package outbox stores events next to business data and relays them to the broker.
package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/mafuky/invoice-hub/internal/platform/event"
)

// Insert writes env into the outbox table inside tx, so the event commits or
// rolls back together with the business rows that caused it.
func Insert(ctx context.Context, tx *sql.Tx, routingKey string, env event.Envelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("outbox: marshal event %s: %w", env.EventID, err)
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO outbox (event_id, routing_key, body) VALUES (?, ?, ?)`,
		env.EventID, routingKey, body)
	if err != nil {
		return fmt.Errorf("outbox: insert event %s (%s): %w", env.EventID, routingKey, err)
	}
	return nil
}
