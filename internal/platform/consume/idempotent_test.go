//go:build lesson && integration

package consume

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
)

func deliveredEnvelope(t *testing.T) event.Envelope {
	t.Helper()
	env, err := event.New("delivery.invoice.delivered", "1001", 3,
		event.DeliveryDone{InvoiceID: 1001, InvoiceVersion: 3}, "trace-consume")
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func TestHandle_DuplicateSkipsFn(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "invoice")
	env := deliveredEnvelope(t)

	calls := 0
	fn := func(*sql.Tx) error { calls++; return nil }

	dup, err := Handle(ctx, db, "invoice-svc", env, fn)
	if err != nil || dup {
		t.Fatalf("first delivery: duplicate=%v err=%v", dup, err)
	}
	dup, err = Handle(ctx, db, "invoice-svc", env, fn)
	if err != nil {
		t.Fatalf("second delivery: %v", err)
	}
	if !dup {
		t.Fatal("second delivery of the same event_id was not reported as duplicate")
	}
	if calls != 1 {
		t.Fatalf("fn ran %d times, want 1", calls)
	}
}

func TestHandle_FnFailsThenRedelivered_RunsAgain(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "invoice")
	env := deliveredEnvelope(t)

	boom := errors.New("out of order, invoice still issuing")
	dup, err := Handle(ctx, db, "invoice-svc", env, func(*sql.Tx) error { return boom })
	if !errors.Is(err, boom) || dup {
		t.Fatalf("failing fn: duplicate=%v err=%v", dup, err)
	}

	calls := 0
	dup, err = Handle(ctx, db, "invoice-svc", env, func(*sql.Tx) error { calls++; return nil })
	if err != nil || dup || calls != 1 {
		t.Fatalf("redelivery after failure: duplicate=%v err=%v calls=%d", dup, err, calls)
	}
}
