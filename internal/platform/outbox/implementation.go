package outbox

import (
	"context"
	"database/sql"

	"github.com/mafuky/invoice-hub/internal/platform/event"
)

// Inserter writes an event in the caller's transaction.
type Inserter func(context.Context, *sql.Tx, string, event.Envelope) error

// ForImplementation selects the writer using the validated store configuration.
func ForImplementation(impl string) Inserter {
	if impl == "gorm" {
		return InsertGorm
	}
	return Insert
}

// Runner polls the outbox and can also run a single pass.
type Runner interface {
	Run(context.Context) error
	RunOnce(context.Context) (int, error)
}

// NewRelayForImplementation selects the relay using the validated store configuration.
func NewRelayForImplementation(db *sql.DB, pub Publisher, name, impl string) Runner {
	r := NewRelay(db, pub, name)
	if impl == "gorm" {
		return &GormRelay{Relay: r}
	}
	return r
}
