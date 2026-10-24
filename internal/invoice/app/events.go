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
	// GaveUp marks a delivery that exhausted its retries and needs an operator.
	GaveUp bool
}

// PlanFor maps a routing key and its payload to the transitions they cause.
// It does no I/O, so the whole mapping can be tested without a database.
func PlanFor(routingKey string, payload []byte) (Plan, error) {
	switch routingKey {
	case mq.ProviderTaskSubmitted:
		var p event.TaskSubmitted
		if err := json.Unmarshal(payload, &p); err != nil {
			return Plan{}, fmt.Errorf("decode %s: %w", routingKey, err)
		}
		return Plan{
			TaskID: p.TaskID, InvoiceID: p.InvoiceID,
			TaskSteps: []TaskStep{
				{domain.TaskPending, domain.TaskRunning},
				{domain.TaskRunning, domain.TaskWaitingCallback},
			},
			SetDeadline: true,
			InvoiceFrom: domain.InvoiceApplying, InvoiceTo: domain.InvoiceIssuing,
		}, nil

	case mq.ProviderTaskSucceeded:
		var p event.TaskOutcome
		if err := json.Unmarshal(payload, &p); err != nil {
			return Plan{}, fmt.Errorf("decode %s: %w", routingKey, err)
		}
		if p.InvoiceNo == "" {
			return Plan{}, fmt.Errorf("%s for task %d carries no invoice_no", routingKey, p.TaskID)
		}
		return Plan{
			TaskID: p.TaskID, InvoiceID: p.InvoiceID,
			TaskSteps:   []TaskStep{{domain.TaskWaitingCallback, domain.TaskDone}},
			InvoiceFrom: domain.InvoiceIssuing, InvoiceTo: domain.InvoiceIssued,
			InvoiceNo: p.InvoiceNo,
			Emit:      mq.InvoiceIssued,
		}, nil

	case mq.ProviderTaskFailed:
		var p event.TaskOutcome
		if err := json.Unmarshal(payload, &p); err != nil {
			return Plan{}, fmt.Errorf("decode %s: %w", routingKey, err)
		}
		return Plan{
			TaskID: p.TaskID, InvoiceID: p.InvoiceID,
			TaskSteps: []TaskStep{{domain.TaskWaitingCallback, domain.TaskFailed}},
		}, nil

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

// Events applies supplier, delivery and settlement events to invoices.
type Events struct {
	db       *sql.DB
	repo     *store.Repo
	deadline time.Duration
	now      func() time.Time
}

// NewEvents builds the event handler; callbackDeadline is how long a supplier
// may take to call back before the timeout scanner steps in.
func NewEvents(db *sql.DB, repo *store.Repo, callbackDeadline time.Duration) *Events {
	return &Events{db: db, repo: repo, deadline: callbackDeadline, now: time.Now}
}

// Handle is the mq.Handler of q.invoice. A returned error sends the event to
// the next retry tier, which is how an event that arrives before its
// predecessor is absorbed.
func (e *Events) Handle(ctx context.Context, env event.Envelope) error {
	plan, err := PlanFor(env.Type, env.Payload)
	if err != nil {
		return err
	}
	_, err = consume.Handle(ctx, e.db, consumerName, env, func(tx *sql.Tx) error {
		return e.apply(ctx, tx, env, plan)
	})
	return err
}

func (e *Events) apply(ctx context.Context, tx *sql.Tx, env event.Envelope, p Plan) error {
	if p.GaveUp {
		slog.ErrorContext(ctx, "delivery gave up, invoice needs an operator",
			"invoice_id", p.InvoiceID, "event_id", env.EventID)
		return nil
	}

	if len(p.TaskSteps) > 0 {
		task, err := e.repo.GetTask(ctx, tx, p.TaskID)
		if err != nil {
			return err
		}
		version := task.Version
		for _, st := range p.TaskSteps {
			ok, err := e.repo.TransitionTask(ctx, tx, p.TaskID, st.From, st.To, version, env.Type, env.EventID)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("task %d is not %s at version %d, %s arrived out of order", p.TaskID, st.From, version, env.Type)
			}
			version++
		}
		if p.SetDeadline {
			if err := e.repo.SetTaskDeadline(ctx, tx, p.TaskID, e.now().Add(e.deadline)); err != nil {
				return err
			}
		}
	}

	if p.InvoiceTo != "" {
		inv, err := e.repo.GetInvoiceTx(ctx, tx, p.InvoiceID)
		if err != nil {
			return err
		}
		ok, err := e.repo.TransitionInvoice(ctx, tx, p.InvoiceID, p.InvoiceFrom, p.InvoiceTo, inv.Version, env.Type, env.EventID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("invoice %d is not %s at version %d, %s arrived out of order", p.InvoiceID, p.InvoiceFrom, inv.Version, env.Type)
		}
		if p.InvoiceNo != "" {
			if err := e.repo.SetInvoiceNo(ctx, tx, p.InvoiceID, p.InvoiceNo); err != nil {
				return err
			}
		}
	}

	if p.Emit != "" {
		return e.emit(ctx, tx, env, p)
	}
	return nil
}

func (e *Events) emit(ctx context.Context, tx *sql.Tx, cause event.Envelope, p Plan) error {
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
	out, err := event.New(p.Emit, strconv.FormatInt(inv.ID, 10), inv.Version, payload, cause.TraceID)
	if err != nil {
		return err
	}
	return outbox.Insert(ctx, tx, p.Emit, out)
}
