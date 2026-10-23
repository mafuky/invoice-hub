package event

import (
	"time"

	"github.com/mafuky/invoice-hub/internal/platform/money"
)

// TaskCreated is published when an issue request has been accepted and a task is waiting for a supplier.
type TaskCreated struct {
	TaskID      int64        `json:"task_id"`
	InvoiceID   int64        `json:"invoice_id"`
	CommandKind string       `json:"command_kind"`
	ProductLine string       `json:"product_line"`
	Title       string       `json:"title"`
	TaxNo       string       `json:"tax_no"`
	Amount      money.Amount `json:"amount"`
	CustomerID  string       `json:"customer_id"`
}

// TaskSubmitted is published once the supplier has accepted the order.
type TaskSubmitted struct {
	TaskID          int64  `json:"task_id"`
	InvoiceID       int64  `json:"invoice_id"`
	Supplier        string `json:"supplier"`
	SupplierOrderNo string `json:"supplier_order_no"`
}

// TaskOutcome carries a supplier callback result, for both succeeded and failed tasks.
type TaskOutcome struct {
	TaskID          int64  `json:"task_id"`
	InvoiceID       int64  `json:"invoice_id"`
	Supplier        string `json:"supplier"`
	SupplierOrderNo string `json:"supplier_order_no"`
	InvoiceNo       string `json:"invoice_no"`
	Reason          string `json:"reason"`
}

// InvoiceIssued is published when an invoice number has been recorded.
type InvoiceIssued struct {
	InvoiceID   int64        `json:"invoice_id"`
	Version     int          `json:"version"`
	InvoiceNo   string       `json:"invoice_no"`
	CustomerID  string       `json:"customer_id"`
	ProductLine string       `json:"product_line"`
	Title       string       `json:"title"`
	Amount      money.Amount `json:"amount"`
}

// DeliveryDone reports the end of delivery, either delivered or given up.
type DeliveryDone struct {
	InvoiceID      int64 `json:"invoice_id"`
	InvoiceVersion int   `json:"invoice_version"`
}

// InvoiceDelivered is published when both delivery to the customer and the finance write-back are done.
type InvoiceDelivered struct {
	InvoiceID   int64        `json:"invoice_id"`
	InvoiceNo   string       `json:"invoice_no"`
	CustomerID  string       `json:"customer_id"`
	ProductLine string       `json:"product_line"`
	Title       string       `json:"title"`
	Amount      money.Amount `json:"amount"`
	DeliveredAt time.Time    `json:"delivered_at"`
}

// ItemAdded is published when a delivered invoice has been added to a settlement.
type ItemAdded struct {
	InvoiceID    int64 `json:"invoice_id"`
	SettlementID int64 `json:"settlement_id"`
}
