package gormstore

import (
	"database/sql"
	"time"

	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/settlement/domain"
)

type settlementRow struct {
	ID           int64                   `gorm:"column:id;primaryKey;autoIncrement"`
	Period       string                  `gorm:"column:period"`
	CustomerID   string                  `gorm:"column:customer_id"`
	Amount       money.Amount            `gorm:"column:amount;type:decimal(18,2)"`
	Status       domain.SettlementStatus `gorm:"column:status"`
	Version      int                     `gorm:"column:version"`
	RejectReason string                  `gorm:"column:reject_reason"`
	CreatedAt    time.Time               `gorm:"column:created_at;->"`
	UpdatedAt    time.Time               `gorm:"column:updated_at;->"`
}

func (settlementRow) TableName() string { return "settlements" }
func (r settlementRow) settlement() domain.Settlement {
	return domain.Settlement{ID: r.ID, Period: r.Period, CustomerID: r.CustomerID, Amount: r.Amount, Status: r.Status, Version: r.Version, RejectReason: r.RejectReason, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt}
}

type itemRow struct {
	ID           int64          `gorm:"column:id;primaryKey;autoIncrement"`
	SettlementID int64          `gorm:"column:settlement_id"`
	InvoiceID    int64          `gorm:"column:invoice_id"`
	ProductLine  string         `gorm:"column:product_line"`
	Amount       money.Amount   `gorm:"column:amount;type:decimal(18,2)"`
	Verified     bool           `gorm:"column:verified"`
	Checks       sql.NullString `gorm:"column:checks;type:json"`
}

func (itemRow) TableName() string { return "settlement_items" }

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
