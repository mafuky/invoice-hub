package app

import (
	"github.com/mafuky/invoice-hub/internal/platform/consume"
	"github.com/mafuky/invoice-hub/internal/platform/outbox"
)

type persistence struct {
	handle consume.Handler
	insert outbox.Inserter
}

// Option configures persistence before the service starts.
type Option func(*persistence)

// WithPersistence binds event handling and event writes to the selected store implementation.
func WithPersistence(handle consume.Handler, insert outbox.Inserter) Option {
	return func(p *persistence) { p.handle = handle; p.insert = insert }
}

func newPersistence(options []Option) persistence {
	p := persistence{handle: consume.Handle, insert: outbox.Insert}
	for _, option := range options {
		option(&p)
	}
	return p
}
