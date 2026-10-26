package gormstore

import (
	"database/sql"
	"time"

	"github.com/mafuky/invoice-hub/internal/provider/store"
)

type orderRow struct {
	ID              int64          `gorm:"column:id;primaryKey;autoIncrement"`
	TaskID          int64          `gorm:"column:task_id"`
	InvoiceID       int64          `gorm:"column:invoice_id"`
	Supplier        string         `gorm:"column:supplier"`
	SupplierOrderNo string         `gorm:"column:supplier_order_no"`
	Status          string         `gorm:"column:status"`
	Outcome         string         `gorm:"column:outcome"`
	InvoiceNo       sql.NullString `gorm:"column:invoice_no"`
	Version         int            `gorm:"column:version"`
	LastEventSeq    int64          `gorm:"column:last_event_seq"`
	CreatedAt       time.Time      `gorm:"column:created_at;->"`
	UpdatedAt       time.Time      `gorm:"column:updated_at;->"`
}

func (orderRow) TableName() string { return "provider_orders" }

func (r orderRow) order() store.Order {
	return store.Order{ID: r.ID, TaskID: r.TaskID, InvoiceID: r.InvoiceID, Supplier: r.Supplier,
		SupplierOrderNo: r.SupplierOrderNo, Status: r.Status, Outcome: r.Outcome,
		InvoiceNo: r.InvoiceNo.String, Version: r.Version, LastEventSeq: r.LastEventSeq}
}
