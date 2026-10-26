package gormstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"database/sql"
	"encoding/json"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/mafuky/invoice-hub/internal/platform/gormx"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/settlement/domain"
	"github.com/mafuky/invoice-hub/internal/settlement/store"
)

type Repo struct {
	db       *sql.DB
	sessions *gormx.Sessions
}

var _ store.Store = (*Repo)(nil)

func NewRepo(db *sql.DB) (*Repo, error) {
	sessions, err := gormx.New(db)
	if err != nil {
		return nil, fmt.Errorf("store: initialize gorm: %w", err)
	}
	return &Repo{db: db, sessions: sessions}, nil
}

func (r *Repo) session(ctx context.Context, q store.DBTX) *gorm.DB {
	conn, ok := q.(gorm.ConnPool)
	db := r.sessions.Session(ctx, conn)
	if !ok {
		db.AddError(errors.New("store: connection must support GORM's connection pool interface"))
	}
	return db
}

func readSettlement(db *gorm.DB) (domain.Settlement, error) {
	var row settlementRow
	if err := db.Take(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.Settlement{}, domain.ErrNotFound
		}
		return domain.Settlement{}, fmt.Errorf("store: read settlement: %w", err)
	}
	return row.settlement(), nil
}

func (r *Repo) GetSettlement(ctx context.Context, id int64) (domain.Settlement, error) {
	return r.GetSettlementTx(ctx, r.db, id)
}
func (r *Repo) GetSettlementTx(ctx context.Context, q store.DBTX, id int64) (domain.Settlement, error) {
	return readSettlement(r.session(ctx, q).Where("id = ?", id))
}
func (r *Repo) GetSettlementForUpdate(ctx context.Context, tx *sql.Tx, id int64) (domain.Settlement, error) {
	return readSettlement(r.session(ctx, tx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", id))
}
func (r *Repo) ListSettlements(ctx context.Context, status string) ([]domain.Settlement, error) {
	db := r.session(ctx, r.db)
	if status != "" {
		db = db.Where("status = ?", status)
	}
	var rows []settlementRow
	if err := db.Order("id DESC").Limit(200).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("store: list settlements: %w", err)
	}
	var out []domain.Settlement
	for _, row := range rows {
		out = append(out, row.settlement())
	}
	return out, nil
}

// FindOrCreateSettlement returns a row locked through the caller's transaction.
func (r *Repo) FindOrCreateSettlement(ctx context.Context, tx *sql.Tx, period, customerID, cause string) (domain.Settlement, error) {
	s, err := readSettlement(r.session(ctx, tx).Where("period = ? AND customer_id = ?", period, customerID))
	if err == nil {
		return r.GetSettlementForUpdate(ctx, tx, s.ID)
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return domain.Settlement{}, err
	}
	row := settlementRow{Period: period, CustomerID: customerID, Status: domain.SettlementCreated}
	err = r.session(ctx, tx).Create(&row).Error
	if store.IsDuplicateKey(err) {
		// A locking read sees a concurrently committed row beyond the snapshot.
		return readSettlement(r.session(ctx, tx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("period = ? AND customer_id = ?", period, customerID))
	}
	if err != nil {
		return domain.Settlement{}, fmt.Errorf("store: insert settlement %s/%s: %w", period, customerID, err)
	}
	if err := r.AppendStateLog(ctx, tx, row.ID, "", string(domain.SettlementCreated), cause); err != nil {
		return domain.Settlement{}, err
	}
	applied, err := r.TransitionSettlement(ctx, tx, row.ID, domain.SettlementCreated, domain.SettlementPendingSettle, 0, cause)
	if err != nil {
		return domain.Settlement{}, err
	}
	if !applied {
		return domain.Settlement{}, fmt.Errorf("settlement %d: new settlement could not be opened: %w", row.ID, domain.ErrRefused)
	}
	return r.GetSettlementForUpdate(ctx, tx, row.ID)
}

// FindOrCreateOpen keeps the selected row locked until the item is committed.
func (r *Repo) FindOrCreateOpen(ctx context.Context, tx *sql.Tx, customerID, period string) (domain.Settlement, error) {
	month, err := time.Parse("2006-01", period)
	if err != nil {
		return domain.Settlement{}, fmt.Errorf("store: invalid settlement period %q: %w", period, err)
	}
	for {
		s, err := r.FindOrCreateSettlement(ctx, tx, month.Format("2006-01"), customerID, "invoice.invoice.delivered")
		if err != nil {
			return domain.Settlement{}, err
		}
		if s.Status == domain.SettlementPendingSettle {
			return s, nil
		}
		month = month.AddDate(0, 1, 0)
	}
}

func (r *Repo) TransitionSettlement(ctx context.Context, tx *sql.Tx, id int64, from, to domain.SettlementStatus, version int, cause string) (bool, error) {
	res := r.session(ctx, tx).Model(&settlementRow{}).Where("id = ? AND status = ? AND version = ?", id, from, version).
		Updates(map[string]any{"status": to, "version": gorm.Expr("version + 1")})
	if res.Error != nil {
		return false, fmt.Errorf("store: transition settlement %d: %w", id, res.Error)
	}
	if res.RowsAffected == 0 {
		return false, nil
	}
	if err := r.AppendStateLog(ctx, tx, id, string(from), string(to), cause); err != nil {
		return false, err
	}
	return true, nil
}
func (r *Repo) AppendStateLog(ctx context.Context, q store.DBTX, id int64, from, to, cause string) error {
	row := stateLogRow{Entity: store.EntitySettlement, EntityID: id, FromStatus: from, ToStatus: to, Cause: cause}
	if err := r.session(ctx, q).Create(&row).Error; err != nil {
		return fmt.Errorf("store: append state_log for settlement %d: %w", id, err)
	}
	return nil
}
func (r *Repo) SetAmount(ctx context.Context, q store.DBTX, id int64, amount money.Amount) error {
	if err := r.session(ctx, q).Model(&settlementRow{}).Where("id = ?", id).Update("amount", amount).Error; err != nil {
		return fmt.Errorf("store: set amount of settlement %d: %w", id, err)
	}
	return nil
}
func (r *Repo) SetRejectReason(ctx context.Context, q store.DBTX, id int64, reason string) error {
	if err := r.session(ctx, q).Model(&settlementRow{}).Where("id = ?", id).Update("reject_reason", reason).Error; err != nil {
		return fmt.Errorf("store: set reject reason of settlement %d: %w", id, err)
	}
	return nil
}
func (r *Repo) InsertItem(ctx context.Context, q store.DBTX, it store.Item) (bool, error) {
	var checks sql.NullString
	if it.Checks != nil {
		raw, err := json.Marshal(it.Checks)
		if err != nil {
			return false, fmt.Errorf("store: encode checks of invoice %d: %w", it.InvoiceID, err)
		}
		checks = sql.NullString{String: string(raw), Valid: true}
	}
	row := itemRow{SettlementID: it.SettlementID, InvoiceID: it.InvoiceID, ProductLine: it.ProductLine, Amount: it.Amount, Verified: it.Verified, Checks: checks}
	err := r.session(ctx, q).Create(&row).Error
	if store.IsDuplicateKey(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: insert item for invoice %d: %w", it.InvoiceID, err)
	}
	return true, nil
}
func (r *Repo) SumItems(ctx context.Context, q store.DBTX, settlementID int64) (money.Amount, error) {
	var total struct {
		Amount money.Amount `gorm:"column:amount"`
	}
	err := r.session(ctx, q).Model(&itemRow{}).Select("COALESCE(SUM(amount), 0) AS amount").Where("settlement_id = ?", settlementID).Scan(&total).Error
	if err != nil {
		return money.Amount{}, fmt.Errorf("store: sum items of settlement %d: %w", settlementID, err)
	}
	return total.Amount, nil
}
func (r *Repo) ListItems(ctx context.Context, settlementID int64) ([]store.Item, error) {
	var rows []itemRow
	if err := r.session(ctx, r.db).Where("settlement_id = ?", settlementID).Order("id").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("store: list items of settlement %d: %w", settlementID, err)
	}
	var out []store.Item
	for _, row := range rows {
		it := store.Item{ID: row.ID, SettlementID: row.SettlementID, InvoiceID: row.InvoiceID, ProductLine: row.ProductLine, Amount: row.Amount, Verified: row.Verified}
		if row.Checks.Valid && row.Checks.String != "" {
			if err := json.Unmarshal([]byte(row.Checks.String), &it.Checks); err != nil {
				return nil, fmt.Errorf("store: decode checks of invoice %d: %w", it.InvoiceID, err)
			}
		}
		out = append(out, it)
	}
	return out, nil
}

// HasItem checks whether an invoice already belongs to a statement before re-verifying it.
func (r *Repo) HasItem(ctx context.Context, q store.DBTX, invoiceID int64) (bool, error) {
	var n int64
	err := r.session(ctx, q).Model(&itemRow{}).Where("invoice_id = ?", invoiceID).Count(&n).Error
	if err != nil {
		return false, fmt.Errorf("store: find item for invoice %d: %w", invoiceID, err)
	}
	return n != 0, nil
}
