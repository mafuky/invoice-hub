package gormstore

import (
	"database/sql"
	"time"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/platform/money"
)

type invoiceRow struct {
	ID          int64                `gorm:"column:id;primaryKey;autoIncrement"`
	BizOrderNo  string               `gorm:"column:biz_order_no"`
	ProductLine string               `gorm:"column:product_line"`
	Title       string               `gorm:"column:title"`
	TaxNo       string               `gorm:"column:tax_no"`
	Amount      money.Amount         `gorm:"column:amount;type:decimal(18,2)"`
	CustomerID  string               `gorm:"column:customer_id"`
	Status      domain.InvoiceStatus `gorm:"column:status"`
	InvoiceNo   sql.NullString       `gorm:"column:invoice_no"`
	Version     int                  `gorm:"column:version"`
	CreatedAt   time.Time            `gorm:"column:created_at;->"`
	UpdatedAt   time.Time            `gorm:"column:updated_at;->"`
}

func (invoiceRow) TableName() string { return "invoices" }

func (r invoiceRow) invoice() domain.Invoice {
	return domain.Invoice{ID: r.ID, BizOrderNo: r.BizOrderNo, ProductLine: r.ProductLine,
		Title: r.Title, TaxNo: r.TaxNo, Amount: r.Amount, CustomerID: r.CustomerID,
		Status: r.Status, InvoiceNo: r.InvoiceNo.String, Version: r.Version,
		CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt}
}

type commandRow struct {
	ID        int64                `gorm:"column:id;primaryKey;autoIncrement"`
	InvoiceID int64                `gorm:"column:invoice_id"`
	Kind      string               `gorm:"column:kind"`
	Status    domain.CommandStatus `gorm:"column:status"`
	Reason    string               `gorm:"column:reason"`
	Version   int                  `gorm:"column:version"`
	CreatedAt time.Time            `gorm:"column:created_at;->"`
}

func (commandRow) TableName() string { return "commands" }

func (r commandRow) command() domain.Command {
	return domain.Command{ID: r.ID, InvoiceID: r.InvoiceID, Kind: r.Kind, Status: r.Status, Reason: r.Reason, Version: r.Version}
}

type taskRow struct {
	ID          int64             `gorm:"column:id;primaryKey;autoIncrement"`
	CommandID   int64             `gorm:"column:command_id"`
	InvoiceID   int64             `gorm:"column:invoice_id"`
	Supplier    sql.NullString    `gorm:"column:supplier"`
	Status      domain.TaskStatus `gorm:"column:status"`
	Attempt     int               `gorm:"column:attempt"`
	NextRetryAt *time.Time        `gorm:"column:next_retry_at"`
	DeadlineAt  *time.Time        `gorm:"column:deadline_at"`
	Version     int               `gorm:"column:version"`
}

func (taskRow) TableName() string { return "tasks" }

func (r taskRow) task() domain.Task {
	return domain.Task{ID: r.ID, CommandID: r.CommandID, InvoiceID: r.InvoiceID, Supplier: r.Supplier.String,
		Status: r.Status, Attempt: r.Attempt, DeadlineAt: r.DeadlineAt, Version: r.Version}
}

type stateLogRow struct {
	ID         int64          `gorm:"column:id;primaryKey;autoIncrement"`
	Entity     string         `gorm:"column:entity"`
	EntityID   int64          `gorm:"column:entity_id"`
	FromStatus string         `gorm:"column:from_status"`
	ToStatus   string         `gorm:"column:to_status"`
	Cause      string         `gorm:"column:cause"`
	EventID    sql.NullString `gorm:"column:event_id"`
	At         time.Time      `gorm:"column:at;->"`
}

func (stateLogRow) TableName() string { return "state_log" }
