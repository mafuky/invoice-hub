// Implemented in lesson 4.

// Package agent turns "task created" events into supplier orders.
package agent

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/provider/client"
	"github.com/mafuky/invoice-hub/internal/provider/store"
)

// ConsumerName identifies provider-svc in processed_events.
const ConsumerName = "provider-svc"

// ErrNoSupplier means every supplier that serves the product line has been excluded.
var ErrNoSupplier = errors.New("no supplier left for the product line")

// SupplierInfo is a supplier and the product lines it can issue invoices for.
type SupplierInfo struct {
	Name  string
	Lines []string
}

// DefaultSuppliers is the fixed supplier list, in order of preference. It
// matches the suppliers of the mock scenarios.
func DefaultSuppliers() []SupplierInfo {
	return []SupplierInfo{
		{Name: "air-a", Lines: []string{"FLIGHT"}},
		{Name: "rail-a", Lines: []string{"TRAIN"}},
		{Name: "hotel-a", Lines: []string{"HOTEL"}},
		{Name: "backup", Lines: []string{"FLIGHT", "TRAIN", "HOTEL"}},
	}
}

// Orderer places orders with a supplier.
type Orderer interface {
	CreateOrder(ctx context.Context, supplier string, req client.OrderRequest) (supplierOrderNo string, err error)
}

// ProviderAgent places supplier orders for new tasks.
type ProviderAgent struct {
	db        *sql.DB
	repo      *store.Repo
	orders    Orderer
	suppliers []SupplierInfo
}

func NewProviderAgent(db *sql.DB, repo *store.Repo, orders Orderer, suppliers []SupplierInfo) *ProviderAgent {
	return &ProviderAgent{db: db, repo: repo, orders: orders, suppliers: suppliers}
}

// ChooseSupplier picks the supplier for one invoice line: the first in the list
// that serves line and is not named in exclude.
//
// Invariant: an excluded supplier is never chosen, and when none is left the
// error is ErrNoSupplier.
//
// If a supplier that already failed this invoice is chosen again, the
// employee's invoice is stuck behind the same refusal and the retry never
// reaches the backup supplier.
func ChooseSupplier(line string, exclude []string, suppliers []SupplierInfo) (SupplierInfo, error) {
	return SupplierInfo{}, fmt.Errorf("lesson 4: %w", errors.ErrUnsupported)
}

// OnTaskCreated handles an invoice.task.created event: it asks a supplier to
// issue the invoice for the task, records the supplier order and announces it
// with a provider.task.submitted event for invoice-svc.
//
// Invariant: a task is ordered at most once, however many times its event is
// delivered, and the order row and the outgoing event commit together. Never
// ask the same supplier twice for one invoice: a supplier whose earlier order
// for the invoice failed or timed out is not used again, and an order still
// unanswered under an older task of the same invoice is presumed timed out,
// because a newer task exists only when the older one was abandoned.
//
// If the event is handled twice, the supplier issues two invoices for one
// trip and the enterprise pays the tax twice; if the order is placed but the
// event is lost, the invoice stays in applying and nobody knows which
// supplier holds it.
func (a *ProviderAgent) OnTaskCreated(ctx context.Context, env event.Envelope) error {
	return fmt.Errorf("lesson 4: %w", errors.ErrUnsupported)
}
