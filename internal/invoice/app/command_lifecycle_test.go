//go:build lesson && integration

package app

import (
	"context"
	"testing"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
)

func assertCommand(t *testing.T, f *invoiceFixture, id int64, status domain.CommandStatus, version int, keys ...string) {
	t.Helper()
	c, err := f.repo.GetCommand(context.Background(), f.db, id)
	if err != nil {
		t.Fatal(err)
	}
	if c.Status != status || c.Version != version {
		t.Fatalf("command=%+v want %s/%d", c, status, version)
	}
	rows, err := f.db.Query(`SELECT from_status,to_status,cause,event_id FROM state_log WHERE entity='command' AND entity_id=? ORDER BY id`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	i := 0
	from := "created"
	for rows.Next() {
		var a, b, key, eventID string
		if err := rows.Scan(&a, &b, &key, &eventID); err != nil {
			t.Fatal(err)
		}
		if i >= len(keys) {
			t.Fatal("extra command log")
		}
		to := "dispatched"
		if i == 1 {
			to = string(status)
		}
		if a != from || b != to || key != keys[i] || eventID == "" {
			t.Fatalf("log=%s %s %s %s", a, b, key, eventID)
		}
		from = b
		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if i != len(keys) {
		t.Fatalf("logs=%d want=%d", i, len(keys))
	}
}

func TestEvents_CommandLifecycle_IssueRetryThenSuccess(t *testing.T) {
	f := seedFixture(t, domain.InvoiceApplying, domain.KindIssue, domain.TaskPending)
	submitted := sendEvent(t, f, mq.ProviderTaskSubmitted, f.task)
	assertCommand(t, f, f.cmd, domain.CommandDispatched, 1, mq.ProviderTaskSubmitted)
	if err := f.events.Handle(context.Background(), submitted); err != nil {
		t.Fatal(err)
	}
	sendEvent(t, f, mq.ProviderTaskFailed, f.task)
	assertCommand(t, f, f.cmd, domain.CommandDispatched, 1, mq.ProviderTaskSubmitted)
	var next int64
	if err := f.db.QueryRow(`SELECT id FROM tasks WHERE command_id=? AND status='pending'`, f.cmd).Scan(&next); err != nil {
		t.Fatal(err)
	}
	sendEvent(t, f, mq.ProviderTaskSubmitted, next)
	assertCommand(t, f, f.cmd, domain.CommandDispatched, 1, mq.ProviderTaskSubmitted)
	// A late success of the failed attempt must not complete this command.
	sendEvent(t, f, mq.ProviderTaskSucceeded, f.task)
	assertCommand(t, f, f.cmd, domain.CommandDispatched, 1, mq.ProviderTaskSubmitted)
	remove := failOutbox(t, f.db)
	env, err := event.New(mq.ProviderTaskSucceeded, "1", 1, event.TaskOutcome{TaskID: next, InvoiceID: f.inv.ID, InvoiceNo: "AIR-A-12345678"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.events.Handle(context.Background(), env); err == nil {
		t.Fatal("outbox failure accepted")
	}
	assertCommand(t, f, f.cmd, domain.CommandDispatched, 1, mq.ProviderTaskSubmitted)
	countRows(t, f.db, `SELECT COUNT(*) FROM tasks WHERE id=? AND status='waiting_callback' AND version=2`, 1, next)
	countRows(t, f.db, `SELECT COUNT(*) FROM invoices WHERE id=? AND status='issuing' AND version=1`, 1, f.inv.ID)
	countRows(t, f.db, `SELECT COUNT(*) FROM processed_events WHERE event_id=?`, 0, env.EventID)
	remove()
	if err := f.events.Handle(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	assertCommand(t, f, f.cmd, domain.CommandSucceeded, 2, mq.ProviderTaskSubmitted, mq.ProviderTaskSucceeded)
	if err := f.events.Handle(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	sendEvent(t, f, mq.ProviderTaskSucceeded, next)
	sendEvent(t, f, mq.ProviderTaskFailed, next)
	assertCommand(t, f, f.cmd, domain.CommandSucceeded, 2, mq.ProviderTaskSubmitted, mq.ProviderTaskSucceeded)
	countRows(t, f.db, `SELECT COUNT(*) FROM outbox WHERE routing_key=?`, 1, mq.InvoiceIssued)
}

func TestEvents_CommandLifecycle_RedFlushFailure(t *testing.T) {
	f := seedFixture(t, domain.InvoiceClosed, domain.KindRedFlush, domain.TaskPending)
	sendEvent(t, f, mq.ProviderTaskSubmitted, f.task)
	assertCommand(t, f, f.cmd, domain.CommandDispatched, 1, mq.ProviderTaskSubmitted)
	failed := sendEvent(t, f, mq.ProviderTaskFailed, f.task)
	assertCommand(t, f, f.cmd, domain.CommandFailed, 2, mq.ProviderTaskSubmitted, mq.ProviderTaskFailed)
	if err := f.events.Handle(context.Background(), failed); err != nil {
		t.Fatal(err)
	}
	sendEvent(t, f, mq.ProviderTaskFailed, f.task)
	sendEvent(t, f, mq.ProviderTaskSucceeded, f.task)
	assertCommand(t, f, f.cmd, domain.CommandFailed, 2, mq.ProviderTaskSubmitted, mq.ProviderTaskFailed)
	countRows(t, f.db, `SELECT COUNT(*) FROM invoices WHERE id=? AND status='red_flushing'`, 1, f.inv.ID)
	graphCounts(t, f.db, 1, 1, 1, 0)
}

func TestEvents_CommandLifecycle_ReissueReplacementAtomic(t *testing.T) {
	f := seedFixture(t, domain.InvoiceClosed, domain.KindReissue, domain.TaskPending)
	sendEvent(t, f, mq.ProviderTaskSubmitted, f.task)
	remove := failOutbox(t, f.db)
	env, err := event.New(mq.ProviderTaskSucceeded, "1", 1, event.TaskOutcome{TaskID: f.task, InvoiceID: f.inv.ID}, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.events.Handle(context.Background(), env); err == nil {
		t.Fatal("replacement outbox failure accepted")
	}
	assertCommand(t, f, f.cmd, domain.CommandDispatched, 1, mq.ProviderTaskSubmitted)
	countRows(t, f.db, `SELECT COUNT(*) FROM tasks WHERE id=? AND status='waiting_callback' AND version=2`, 1, f.task)
	countRows(t, f.db, `SELECT COUNT(*) FROM invoices WHERE id=? AND status='red_flushing' AND version=1`, 1, f.inv.ID)
	countRows(t, f.db, `SELECT COUNT(*) FROM processed_events WHERE event_id=?`, 0, env.EventID)
	remove()
	graphCounts(t, f.db, 1, 1, 1, 0)
	if err := f.events.Handle(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	assertCommand(t, f, f.cmd, domain.CommandSucceeded, 2, mq.ProviderTaskSubmitted, mq.ProviderTaskSucceeded)
	countRows(t, f.db, `SELECT COUNT(*) FROM commands WHERE invoice_id<>? AND kind='ISSUE' AND status='created' AND version=0`, 1, f.inv.ID)
	if err := f.events.Handle(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	sendEvent(t, f, mq.ProviderTaskSucceeded, f.task)
	sendEvent(t, f, mq.ProviderTaskFailed, f.task)
	assertCommand(t, f, f.cmd, domain.CommandSucceeded, 2, mq.ProviderTaskSubmitted, mq.ProviderTaskSucceeded)
	graphCounts(t, f.db, 2, 2, 2, 1)
}
