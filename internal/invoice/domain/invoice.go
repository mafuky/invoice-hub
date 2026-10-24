package domain

import (
	"time"

	"github.com/mafuky/invoice-hub/internal/platform/money"
)

type Invoice struct {
	ID          int64
	BizOrderNo  string
	ProductLine string
	Title       string
	TaxNo       string
	Amount      money.Amount
	CustomerID  string
	Status      InvoiceStatus
	InvoiceNo   string
	Version     int
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type Command struct {
	ID        int64
	InvoiceID int64
	Kind      string
	Status    CommandStatus
	Reason    string
}

type Task struct {
	ID         int64
	CommandID  int64
	InvoiceID  int64
	Supplier   string
	Status     TaskStatus
	Attempt    int
	DeadlineAt *time.Time
	Version    int
}
