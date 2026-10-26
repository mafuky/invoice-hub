package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/invoice/store"
	"github.com/mafuky/invoice-hub/internal/platform/consume"
	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
	"github.com/mafuky/invoice-hub/internal/platform/outbox"
)

const consumerName = "invoice-svc"

// TaskStep is one task state move.
type TaskStep struct {
	From, To domain.TaskStatus
}

// Plan is what an incoming event does to the task and the invoice it names.
// Empty fields mean "leave unchanged".
type Plan struct {
	TaskID    int64
	InvoiceID int64

	TaskSteps   []TaskStep
	SetDeadline bool

	InvoiceFrom domain.InvoiceStatus
	InvoiceTo   domain.InvoiceStatus
	InvoiceNo   string

	// Emit is the routing key of the event to write to the outbox once the invoice has moved.
	Emit string
	// Reopen asks for a replacement task after a failed issue attempt.
	Reopen bool
	// IssueReplacement asks for the new invoice of a reissue once the old one is red flushed.
	IssueReplacement bool
	// Ack drops a duplicate outcome of a task that is already finished.
	Ack bool
	// Flag drops a late success of a task that was already given up, and records it for operators.
	Flag bool
	// GaveUp marks a delivery that exhausted its retries and needs an operator.
	GaveUp bool
	// NeedsOperator marks a failed red flush: the invoice stays as it is until someone handles it.
	NeedsOperator bool
}

// PlanFor maps a routing key and its payload to the transitions they cause.
// The command kind of the task and the invoice's current state decide which
// path applies: issuing (ISSUE, SUPPLEMENT) or red flush (RED_FLUSH, REISSUE).
// The task's current status separates new outcomes from duplicates and late ones.
// It does no I/O, so the whole mapping can be tested without a database.
func PlanFor(routingKey string, payload []byte, invoiceStatus domain.InvoiceStatus, kind string, taskStatus domain.TaskStatus) (Plan, error) {
	switch routingKey {
	case mq.ProviderTaskSubmitted:
		var p event.TaskSubmitted
		if err := json.Unmarshal(payload, &p); err != nil {
			return Plan{}, fmt.Errorf("decode %s: %w", routingKey, err)
		}
		plan := Plan{
			TaskID: p.TaskID, InvoiceID: p.InvoiceID,
			TaskSteps: []TaskStep{
				{domain.TaskPending, domain.TaskRunning},
				{domain.TaskRunning, domain.TaskWaitingCallback},
			},
			SetDeadline: true,
		}
		switch {
		case isIssueKind(kind):
			// A reopened task finds the invoice already issuing.
			if invoiceStatus != domain.InvoiceIssuing {
				plan.InvoiceFrom, plan.InvoiceTo = domain.InvoiceApplying, domain.InvoiceIssuing
			}
		case isRedKind(kind):
			if invoiceStatus != domain.InvoiceRedFlushing {
				plan.InvoiceFrom, plan.InvoiceTo = domain.InvoiceClosed, domain.InvoiceRedFlushing
			}
		default:
			return Plan{}, fmt.Errorf("%s for task %d: unknown command kind %q", routingKey, p.TaskID, kind)
		}
		return plan, nil

	case mq.ProviderTaskSucceeded:
		var p event.TaskOutcome
		if err := json.Unmarshal(payload, &p); err != nil {
			return Plan{}, fmt.Errorf("decode %s: %w", routingKey, err)
		}
		if taskStatus == domain.TaskDone {
			return Plan{TaskID: p.TaskID, InvoiceID: p.InvoiceID, Ack: true}, nil
		}
		if taskStatus == domain.TaskFailed {
			return Plan{TaskID: p.TaskID, InvoiceID: p.InvoiceID, Flag: true}, nil
		}
		plan := Plan{
			TaskID: p.TaskID, InvoiceID: p.InvoiceID,
			TaskSteps: []TaskStep{{domain.TaskWaitingCallback, domain.TaskDone}},
		}
		switch {
		case isIssueKind(kind):
			if p.InvoiceNo == "" {
				return Plan{}, fmt.Errorf("%s for task %d carries no invoice_no", routingKey, p.TaskID)
			}
			plan.InvoiceFrom, plan.InvoiceTo = domain.InvoiceIssuing, domain.InvoiceIssued
			plan.InvoiceNo = p.InvoiceNo
			plan.Emit = mq.InvoiceIssued
		case isRedKind(kind):
			plan.InvoiceFrom, plan.InvoiceTo = domain.InvoiceRedFlushing, domain.InvoiceRedFlushed
			plan.IssueReplacement = kind == domain.KindReissue
		default:
			return Plan{}, fmt.Errorf("%s for task %d: unknown command kind %q", routingKey, p.TaskID, kind)
		}
		return plan, nil

	case mq.ProviderTaskFailed:
		var p event.TaskOutcome
		if err := json.Unmarshal(payload, &p); err != nil {
			return Plan{}, fmt.Errorf("decode %s: %w", routingKey, err)
		}
		if taskStatus == domain.TaskDone || taskStatus == domain.TaskFailed {
			return Plan{TaskID: p.TaskID, InvoiceID: p.InvoiceID, Ack: true}, nil
		}
		plan := Plan{
			TaskID: p.TaskID, InvoiceID: p.InvoiceID,
			TaskSteps: []TaskStep{{domain.TaskWaitingCallback, domain.TaskFailed}},
		}
		switch {
		case isIssueKind(kind):
			plan.Reopen = true
		case isRedKind(kind):
			plan.NeedsOperator = true
		default:
			return Plan{}, fmt.Errorf("%s for task %d: unknown command kind %q", routingKey, p.TaskID, kind)
		}
		return plan, nil

	case mq.DeliverySucceeded:
		var p event.DeliveryDone
		if err := json.Unmarshal(payload, &p); err != nil {
			return Plan{}, fmt.Errorf("decode %s: %w", routingKey, err)
		}
		return Plan{
			InvoiceID:   p.InvoiceID,
			InvoiceFrom: domain.InvoiceIssued, InvoiceTo: domain.InvoiceDelivered,
			Emit: mq.InvoiceDelivered,
		}, nil

	case mq.DeliveryGaveUp:
		var p event.DeliveryDone
		if err := json.Unmarshal(payload, &p); err != nil {
			return Plan{}, fmt.Errorf("decode %s: %w", routingKey, err)
		}
		return Plan{InvoiceID: p.InvoiceID, GaveUp: true}, nil

	case mq.SettlementItemAdded:
		var p event.ItemAdded
		if err := json.Unmarshal(payload, &p); err != nil {
			return Plan{}, fmt.Errorf("decode %s: %w", routingKey, err)
		}
		return Plan{
			InvoiceID:   p.InvoiceID,
			InvoiceFrom: domain.InvoiceDelivered, InvoiceTo: domain.InvoiceClosed,
		}, nil
	}
	return Plan{}, fmt.Errorf("no handler for event type %q", routingKey)
}

func isIssueKind(kind string) bool {
	return kind == domain.KindIssue || kind == domain.KindSupplement
}

func isRedKind(kind string) bool {
	return kind == domain.KindRedFlush || kind == domain.KindReissue
}

// refs pulls the task and invoice ids every invoice-related payload carries.
func refs(payload []byte) (taskID, invoiceID int64, err error) {
	var r struct {
		TaskID    int64 `json:"task_id"`
		InvoiceID int64 `json:"invoice_id"`
	}
	if err := json.Unmarshal(payload, &r); err != nil {
		return 0, 0, fmt.Errorf("decode payload ids: %w", err)
	}
	return r.TaskID, r.InvoiceID, nil
}

// Events applies supplier, delivery and settlement events to invoices.
type Events struct {
	db       *sql.DB
	repo     *store.Repo
	svc      *Service
	deadline time.Duration
	now      func() time.Time
}

// NewEvents builds the event handler; callbackDeadline is how long a supplier
// may take to call back before the timeout scanner steps in.
func NewEvents(db *sql.DB, repo *store.Repo, callbackDeadline time.Duration) *Events {
	return &Events{db: db, repo: repo, svc: NewService(db, repo), deadline: callbackDeadline, now: time.Now}
}

// Handle is the mq.Handler of q.invoice. A returned error sends the event to
// the next retry tier, which is how an event that arrives before its
// predecessor is absorbed.
func (e *Events) Handle(ctx context.Context, env event.Envelope) error {
	_, err := consume.Handle(ctx, e.db, consumerName, env, func(tx *sql.Tx) error {
		return e.applyPayload(ctx, tx, env.Type, env.Payload, env.EventID, env.TraceID)
	})
	return err
}

// ApplyOutcome applies a supplier result found by other means, such as a
// QueryOrder answer, exactly as the matching provider.task.succeeded or
// provider.task.failed event would: task move, invoice move, invoice_no and
// the issued event, all inside tx. routingKey must be one of those two.
func (e *Events) ApplyOutcome(ctx context.Context, tx *sql.Tx, routingKey string, out event.TaskOutcome, eventID, traceID string) error {
	if routingKey != mq.ProviderTaskSucceeded && routingKey != mq.ProviderTaskFailed {
		return fmt.Errorf("apply outcome: %q is not a task outcome", routingKey)
	}
	payload, err := json.Marshal(out)
	if err != nil {
		return fmt.Errorf("apply outcome: %w", err)
	}
	return e.applyPayload(ctx, tx, routingKey, payload, eventID, traceID)
}

func (e *Events) applyPayload(ctx context.Context, tx *sql.Tx, key string, payload []byte, eventID, traceID string) error {
	taskID, invoiceID, err := refs(payload)
	if err != nil {
		return err
	}

	var (
		task domain.Task
		kind string
	)
	if taskID != 0 {
		if task, err = e.repo.GetTaskForUpdate(ctx, tx, taskID); err != nil {
			return err
		}
		cmd, err := e.repo.GetCommandForUpdate(ctx, tx, task.CommandID)
		if err != nil {
			return err
		}
		kind = cmd.Kind
	}
	inv, err := e.repo.GetInvoiceForUpdate(ctx, tx, invoiceID)
	if err != nil {
		return err
	}

	p, err := PlanFor(key, payload, inv.Status, kind, task.Status)
	if err != nil {
		return err
	}
	return e.execute(ctx, tx, p, task, inv, key, eventID, traceID)
}

func (e *Events) execute(ctx context.Context, tx *sql.Tx, p Plan, task domain.Task, inv domain.Invoice, key, eventID, traceID string) error {
	switch {
	case p.Ack:
		slog.InfoContext(ctx, "outcome for a finished task, ignored",
			"task_id", p.TaskID, "invoice_id", p.InvoiceID, "event_id", eventID, "type", key)
		return nil
	case p.Flag:
		slog.ErrorContext(ctx, "supplier issued an invoice for a task already given up, possible double issue",
			"task_id", p.TaskID, "invoice_id", p.InvoiceID, "event_id", eventID)
		return e.repo.AppendStateLog(ctx, tx, store.EntityTask, p.TaskID,
			string(domain.TaskFailed), string(domain.TaskFailed), "late_outcome", eventID)
	}
	if p.GaveUp {
		slog.ErrorContext(ctx, "delivery gave up, invoice needs an operator",
			"invoice_id", p.InvoiceID, "event_id", eventID)
		return nil
	}

	version := task.Version
	for _, st := range p.TaskSteps {
		ok, err := e.repo.TransitionTask(ctx, tx, p.TaskID, st.From, st.To, version, key, eventID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("task %d is not %s at version %d, %s arrived out of order", p.TaskID, st.From, version, key)
		}
		version++
	}
	if p.TaskID != 0 {
		cmd, err := e.repo.GetCommandForUpdate(ctx, tx, task.CommandID)
		if err != nil {
			return err
		}
		var to domain.CommandStatus
		switch {
		case key == mq.ProviderTaskSubmitted && cmd.Status == domain.CommandCreated:
			to = domain.CommandDispatched
		case key == mq.ProviderTaskSucceeded:
			to = domain.CommandSucceeded
		case key == mq.ProviderTaskFailed && isRedKind(cmd.Kind):
			to = domain.CommandFailed
		}
		if to != "" {
			ok, err := e.repo.TransitionCommand(ctx, tx, cmd.ID, cmd.Status, to, cmd.Version, key, eventID)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("command %d changed while applying %s", cmd.ID, key)
			}
		}
	}
	if p.Reopen {
		if _, err := e.svc.Reopen(ctx, tx, p.InvoiceID, task.CommandID, p.TaskID); err != nil {
			return err
		}
	}
	if p.SetDeadline {
		if err := e.repo.SetTaskDeadline(ctx, tx, p.TaskID, e.now().Add(e.deadline)); err != nil {
			return err
		}
	}
	if p.NeedsOperator {
		slog.ErrorContext(ctx, "red flush failed at the supplier, invoice needs an operator",
			"invoice_id", p.InvoiceID, "task_id", p.TaskID, "event_id", eventID)
	}

	if p.InvoiceTo != "" {
		ok, err := e.repo.TransitionInvoice(ctx, tx, p.InvoiceID, p.InvoiceFrom, p.InvoiceTo, inv.Version, key, eventID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("invoice %d is not %s at version %d, %s arrived out of order", p.InvoiceID, p.InvoiceFrom, inv.Version, key)
		}
		if p.InvoiceNo != "" {
			if err := e.repo.SetInvoiceNo(ctx, tx, p.InvoiceID, p.InvoiceNo); err != nil {
				return err
			}
		}
	}

	if p.IssueReplacement {
		if _, err := e.svc.IssueReplacement(ctx, tx, p.InvoiceID, task.CommandID); err != nil {
			return err
		}
	}
	if p.Emit != "" {
		return e.emit(ctx, tx, p, traceID)
	}
	return nil
}

func (e *Events) emit(ctx context.Context, tx *sql.Tx, p Plan, traceID string) error {
	inv, err := e.repo.GetInvoiceTx(ctx, tx, p.InvoiceID)
	if err != nil {
		return err
	}
	var payload any
	switch p.Emit {
	case mq.InvoiceIssued:
		payload = event.InvoiceIssued{
			InvoiceID: inv.ID, Version: inv.Version, InvoiceNo: inv.InvoiceNo,
			CustomerID: inv.CustomerID, ProductLine: inv.ProductLine, Title: inv.Title, Amount: inv.Amount,
		}
	case mq.InvoiceDelivered:
		payload = event.InvoiceDelivered{
			InvoiceID: inv.ID, InvoiceNo: inv.InvoiceNo,
			CustomerID: inv.CustomerID, ProductLine: inv.ProductLine, Title: inv.Title, Amount: inv.Amount,
			DeliveredAt: e.now().UTC(),
		}
	default:
		return fmt.Errorf("cannot emit %q", p.Emit)
	}
	out, err := event.New(p.Emit, strconv.FormatInt(inv.ID, 10), inv.Version, payload, traceID)
	if err != nil {
		return err
	}
	return outbox.Insert(ctx, tx, p.Emit, out)
}
