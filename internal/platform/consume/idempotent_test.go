//go:build lesson && integration

package consume

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"

	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/outbox"
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

func writeEffects(ctx context.Context, tx *sql.Tx, env event.Envelope) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO invoices (biz_order_no,product_line,title,tax_no,amount,customer_id,status) VALUES (?,'HOTEL','Example Travel Co., Ltd.','91310000EXAMPLE001','100.10','CUST-0002','applying')`, testenv.BizOrderNo()); err != nil {
		return err
	}
	return outbox.Insert(ctx, tx, env.Type, env)
}

func assertEffects(t *testing.T, db *sql.DB, env event.Envelope, want int) {
	t.Helper()
	var invoices int
	if err := db.QueryRow(`SELECT COUNT(*) FROM invoices`).Scan(&invoices); err != nil {
		t.Fatal(err)
	}
	if invoices != want {
		t.Fatalf("business invoices=%d want=%d", invoices, want)
	}
	for _, table := range []string{"outbox", "processed_events"} {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM `+table+` WHERE event_id=?`, env.EventID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != want {
			t.Fatalf("%s=%d want=%d", table, n, want)
		}
	}
}

func TestHandle_FnFailureRollsBackEffectsAndMarker(t *testing.T) {
	forEachImplementation(t, func(t *testing.T, handle func(context.Context, *sql.DB, string, event.Envelope, func(*sql.Tx) error) (bool, error)) {
		ctx := context.Background()
		db := testenv.OpenMySQL(t, "invoice")
		env := deliveredEnvelope(t)
		boom := errors.New("business write failed")
		dup, err := handle(ctx, db, "invoice-svc", env, func(tx *sql.Tx) error {
			if err := writeEffects(ctx, tx, env); err != nil {
				return err
			}
			return boom
		})
		if dup || !errors.Is(err, boom) {
			t.Fatalf("duplicate=%v err=%v", dup, err)
		}
		assertEffects(t, db, env, 0)
		dup, err = handle(ctx, db, "invoice-svc", env, func(tx *sql.Tx) error { return writeEffects(ctx, tx, env) })
		if dup || err != nil {
			t.Fatalf("retry duplicate=%v err=%v", dup, err)
		}
		assertEffects(t, db, env, 1)
	})
}

func TestHandle_ConcurrentDuplicateAppliesOnce(t *testing.T) {
	forEachImplementation(t, func(t *testing.T, handle func(context.Context, *sql.DB, string, event.Envelope, func(*sql.Tx) error) (bool, error)) {
		ctx := context.Background()
		db := testenv.OpenMySQL(t, "invoice")
		env := deliveredEnvelope(t)
		const n = 10
		dups := make([]bool, n)
		errs := make([]error, n)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				dups[i], errs[i] = handle(ctx, db, "invoice-svc", env, func(tx *sql.Tx) error { return writeEffects(ctx, tx, env) })
			}()
		}
		close(start)
		wg.Wait()
		first := 0
		for i, err := range errs {
			if err != nil {
				t.Fatal(err)
			}
			if !dups[i] {
				first++
			}
		}
		if first != 1 {
			t.Fatalf("nonduplicates=%d", first)
		}
		assertEffects(t, db, env, 1)
	})
}

func TestHandle_DuplicateSkipsFn(t *testing.T) {
	forEachImplementation(t, func(t *testing.T, handle func(context.Context, *sql.DB, string, event.Envelope, func(*sql.Tx) error) (bool, error)) {
		ctx := context.Background()
		db := testenv.OpenMySQL(t, "invoice")
		env := deliveredEnvelope(t)

		calls := 0
		fn := func(*sql.Tx) error { calls++; return nil }

		dup, err := handle(ctx, db, "invoice-svc", env, fn)
		if err != nil || dup {
			t.Fatalf("first delivery: duplicate=%v err=%v", dup, err)
		}
		dup, err = handle(ctx, db, "invoice-svc", env, fn)
		if err != nil {
			t.Fatalf("second delivery: %v", err)
		}
		if !dup {
			t.Fatal("second delivery of the same event_id was not reported as duplicate")
		}
		if calls != 1 {
			t.Fatalf("fn ran %d times, want 1", calls)
		}
	})
}

func TestHandle_FnFailsThenRedelivered_RunsAgain(t *testing.T) {
	forEachImplementation(t, func(t *testing.T, handle func(context.Context, *sql.DB, string, event.Envelope, func(*sql.Tx) error) (bool, error)) {
		ctx := context.Background()
		db := testenv.OpenMySQL(t, "invoice")
		env := deliveredEnvelope(t)

		boom := errors.New("out of order, invoice still issuing")
		dup, err := handle(ctx, db, "invoice-svc", env, func(*sql.Tx) error { return boom })
		if !errors.Is(err, boom) || dup {
			t.Fatalf("failing fn: duplicate=%v err=%v", dup, err)
		}

		calls := 0
		dup, err = handle(ctx, db, "invoice-svc", env, func(*sql.Tx) error { calls++; return nil })
		if err != nil || dup || calls != 1 {
			t.Fatalf("redelivery after failure: duplicate=%v err=%v calls=%d", dup, err, calls)
		}
	})
}

func forEachImplementation(t *testing.T, test func(*testing.T, func(context.Context, *sql.DB, string, event.Envelope, func(*sql.Tx) error) (bool, error))) {
	t.Helper()
	for _, impl := range []struct {
		name string
		run  func(context.Context, *sql.DB, string, event.Envelope, func(*sql.Tx) error) (bool, error)
	}{{"sql", ForImplementation("sql")}, {"gorm", ForImplementation("gorm")}} {
		t.Run(impl.name, func(t *testing.T) { test(t, impl.run) })
	}
}
