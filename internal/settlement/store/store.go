package store

import (
	"context"

	"database/sql"

	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/settlement/domain"
)

// Store is the persistence boundary used by application and gRPC handlers.
type Store interface {
	HasItem(context.Context, DBTX, int64) (bool, error)
	GetSettlement(ctx context.Context, id int64) (domain.Settlement, error)
	GetSettlementTx(ctx context.Context, q DBTX, id int64) (domain.Settlement, error)
	GetSettlementForUpdate(ctx context.Context, tx *sql.Tx, id int64) (domain.Settlement, error)
	ListSettlements(ctx context.Context, status string) ([]domain.Settlement, error)
	FindOrCreateSettlement(ctx context.Context, tx *sql.Tx, period, customerID, cause string) (domain.Settlement, error)
	FindOrCreateOpen(ctx context.Context, tx *sql.Tx, customerID, period string) (domain.Settlement, error)
	TransitionSettlement(ctx context.Context, tx *sql.Tx, id int64, from, to domain.SettlementStatus, version int, cause string) (applied bool, err error)
	AppendStateLog(ctx context.Context, q DBTX, id int64, from, to, cause string) error
	SetAmount(ctx context.Context, q DBTX, id int64, amount money.Amount) error
	SetRejectReason(ctx context.Context, q DBTX, id int64, reason string) error
	InsertItem(ctx context.Context, q DBTX, it Item) (inserted bool, err error)
	SumItems(ctx context.Context, q DBTX, settlementID int64) (money.Amount, error)
	ListItems(ctx context.Context, settlementID int64) ([]Item, error)
}

var _ Store = (*Repo)(nil)
