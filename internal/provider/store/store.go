package store

import (
	"context"
	"database/sql"
)

// Store is the persistence boundary used by application and gRPC handlers.
type Store interface {
	InsertOrder(ctx context.Context, q DBTX, o NewOrder) (inserted bool, err error)
	OrderByTask(ctx context.Context, taskID int64) (Order, error)
	GetByTask(ctx context.Context, q DBTX, taskID int64) (Order, error)
	GetBySupplierOrder(ctx context.Context, q DBTX, supplier, supplierOrderNo string) (Order, error)
	GetBySupplierOrderForUpdate(ctx context.Context, tx *sql.Tx, supplier, supplierOrderNo string) (Order, error)
	ExcludedSuppliers(ctx context.Context, q DBTX, invoiceID, taskID int64) ([]string, error)
	ApplyOutcome(ctx context.Context, tx *sql.Tx, orderID int64, version int, seq int64, status, outcome, invoiceNo string) (applied bool, err error)
}

var _ Store = (*Repo)(nil)
