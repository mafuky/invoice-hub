package app

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/outbox"
)

func TestEvents_HandleUsesInjectedHandler(t *testing.T) {
	want := errors.New("handler failure")
	env := event.Envelope{EventID: "11111111-1111-1111-1111-111111111111"}
	called := false
	handle := func(ctx context.Context, db *sql.DB, consumer string, got event.Envelope, fn func(*sql.Tx) error) (bool, error) {
		called = true
		if consumer != consumerName || got.EventID != env.EventID {
			t.Fatalf("consumer=%s event=%s", consumer, got.EventID)
		}
		return false, want
	}
	e := NewEvents(nil, nil, time.Minute, WithPersistence(handle, outbox.Insert))
	err := e.Handle(context.Background(), env)
	if !called {
		t.Fatal("injected handler was bypassed")
	}
	if !errors.Is(err, want) {
		t.Fatalf("handler error: %v", err)
	}
}
