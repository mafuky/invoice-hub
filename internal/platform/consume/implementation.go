package consume

import (
	"context"
	"database/sql"

	"github.com/mafuky/invoice-hub/internal/platform/event"
)

// Handler records an event together with its effects in one transaction.
type Handler func(context.Context, *sql.DB, string, event.Envelope, func(*sql.Tx) error) (bool, error)

// ForImplementation selects the handler using the validated store configuration.
func ForImplementation(impl string) Handler {
	if impl == "gorm" {
		return HandleGorm
	}
	return Handle
}
