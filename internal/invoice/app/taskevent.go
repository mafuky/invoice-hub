package app

import (
	"strconv"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
)

// taskCreatedEvent builds the event that asks provider-svc to place an order for a task.
func taskCreatedEvent(inv domain.Invoice, taskID int64, kind, traceID string) (event.Envelope, error) {
	return event.New(mq.InvoiceTaskCreated, strconv.FormatInt(inv.ID, 10), inv.Version, event.TaskCreated{
		TaskID:      taskID,
		InvoiceID:   inv.ID,
		CommandKind: kind,
		ProductLine: inv.ProductLine,
		Title:       inv.Title,
		TaxNo:       inv.TaxNo,
		Amount:      inv.Amount,
		CustomerID:  inv.CustomerID,
	}, traceID)
}
