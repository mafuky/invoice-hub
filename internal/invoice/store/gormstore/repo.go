// Package gormstore persists invoices through GORM using caller-owned transactions.
package gormstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/invoice/store"
	"github.com/mafuky/invoice-hub/internal/platform/gormx"
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

func (r *Repo) GetInvoice(ctx context.Context, id int64) (domain.Invoice, error) {
	return r.GetInvoiceTx(ctx, r.db, id)
}

func (r *Repo) GetInvoiceTx(ctx context.Context, q store.DBTX, id int64) (domain.Invoice, error) {
	return r.getInvoice(r.session(ctx, q), id)
}

func (r *Repo) GetInvoiceForUpdate(ctx context.Context, tx *sql.Tx, id int64) (domain.Invoice, error) {
	return r.getInvoice(r.session(ctx, tx).Clauses(clause.Locking{Strength: "UPDATE"}), id)
}

func (r *Repo) getInvoice(db *gorm.DB, id int64) (domain.Invoice, error) {
	var row invoiceRow
	err := db.Where("id = ?", id).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return domain.Invoice{}, fmt.Errorf("invoice %d: %w", id, domain.ErrNotFound)
	}
	if err != nil {
		return domain.Invoice{}, fmt.Errorf("store: get invoice %d: %w", id, err)
	}
	return row.invoice(), nil
}

func (r *Repo) GetInvoiceByBizOrderNo(ctx context.Context, q store.DBTX, bizOrderNo string) (domain.Invoice, error) {
	var row invoiceRow
	err := r.session(ctx, q).Where("biz_order_no = ?", bizOrderNo).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return domain.Invoice{}, fmt.Errorf("invoice for %s: %w", bizOrderNo, domain.ErrNotFound)
	}
	if err != nil {
		return domain.Invoice{}, fmt.Errorf("store: get invoice by biz_order_no: %w", err)
	}
	return row.invoice(), nil
}

func (r *Repo) ListInvoices(ctx context.Context, status string, afterID int64, limit int) ([]domain.Invoice, error) {
	if limit < 0 {
		return nil, errors.New("store: list invoices: negative limit")
	}
	db := r.session(ctx, r.db).Where("id > ?", afterID)
	if status != "" {
		db = db.Where("status = ?", status)
	}
	var rows []invoiceRow
	if err := db.Order("id").Limit(limit).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("store: list invoices: %w", err)
	}
	var out []domain.Invoice
	for _, row := range rows {
		out = append(out, row.invoice())
	}
	return out, nil
}

func (r *Repo) InsertInvoice(ctx context.Context, q store.DBTX, in store.NewInvoice) (int64, error) {
	row := invoiceRow{BizOrderNo: in.BizOrderNo, ProductLine: in.ProductLine, Title: in.Title,
		TaxNo: in.TaxNo, Amount: in.Amount, CustomerID: in.CustomerID, Status: domain.InvoiceApplying}
	if err := r.session(ctx, q).Create(&row).Error; err != nil {
		return 0, fmt.Errorf("store: insert invoice %s: %w", in.BizOrderNo, err)
	}
	return row.ID, nil
}

func (r *Repo) InsertCommand(ctx context.Context, q store.DBTX, invoiceID int64, kind, reason string) (int64, error) {
	row := commandRow{InvoiceID: invoiceID, Kind: kind, Status: domain.CommandCreated, Reason: reason}
	if err := r.session(ctx, q).Create(&row).Error; err != nil {
		return 0, fmt.Errorf("store: insert command for invoice %d: %w", invoiceID, err)
	}
	return row.ID, nil
}

func (r *Repo) InsertTask(ctx context.Context, q store.DBTX, commandID, invoiceID int64, supplier string) (int64, error) {
	row := taskRow{CommandID: commandID, InvoiceID: invoiceID, Supplier: sql.NullString{String: supplier, Valid: supplier != ""}, Status: domain.TaskPending}
	if err := r.session(ctx, q).Create(&row).Error; err != nil {
		return 0, fmt.Errorf("store: insert task for invoice %d: %w", invoiceID, err)
	}
	return row.ID, nil
}

func (r *Repo) GetTask(ctx context.Context, q store.DBTX, id int64) (domain.Task, error) {
	return r.getTask(r.session(ctx, q), id)
}

func (r *Repo) GetTaskForUpdate(ctx context.Context, tx *sql.Tx, id int64) (domain.Task, error) {
	return r.getTask(r.session(ctx, tx).Clauses(clause.Locking{Strength: "UPDATE"}), id)
}

func (r *Repo) getTask(db *gorm.DB, id int64) (domain.Task, error) {
	var row taskRow
	err := db.Where("id = ?", id).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return domain.Task{}, fmt.Errorf("task %d: %w", id, domain.ErrNotFound)
	}
	if err != nil {
		return domain.Task{}, fmt.Errorf("store: get task %d: %w", id, err)
	}
	return row.task(), nil
}

func (r *Repo) SetInvoiceNo(ctx context.Context, q store.DBTX, id int64, invoiceNo string) error {
	res := r.session(ctx, q).Model(&invoiceRow{}).Where("id = ?", id).Update("invoice_no", invoiceNo)
	if res.Error != nil {
		return fmt.Errorf("store: set invoice_no on %d: %w", id, res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("invoice %d: %w", id, domain.ErrNotFound)
	}
	return nil
}

func (r *Repo) SetTaskDeadline(ctx context.Context, q store.DBTX, id int64, at time.Time) error {
	if err := r.session(ctx, q).Model(&taskRow{}).Where("id = ?", id).Update("deadline_at", at.UTC()).Error; err != nil {
		return fmt.Errorf("store: set deadline on task %d: %w", id, err)
	}
	return nil
}

func (r *Repo) ListTimedOutTasks(ctx context.Context, now time.Time, limit int) ([]domain.Task, error) {
	if limit < 0 {
		return nil, errors.New("store: list timed-out tasks: negative limit")
	}
	var rows []taskRow
	err := r.session(ctx, r.db).Where("status = ? AND deadline_at <= ?", domain.TaskWaitingCallback, now.UTC()).Order("deadline_at").Limit(limit).Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("store: list timed-out tasks: %w", err)
	}
	var out []domain.Task
	for _, row := range rows {
		out = append(out, row.task())
	}
	return out, nil
}

func (r *Repo) GetCommand(ctx context.Context, q store.DBTX, id int64) (domain.Command, error) {
	return r.getCommand(r.session(ctx, q), id)
}

func (r *Repo) GetCommandForUpdate(ctx context.Context, tx *sql.Tx, id int64) (domain.Command, error) {
	return r.getCommand(r.session(ctx, tx).Clauses(clause.Locking{Strength: "UPDATE"}), id)
}

func (r *Repo) getCommand(db *gorm.DB, id int64) (domain.Command, error) {
	var row commandRow
	err := db.Where("id = ?", id).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return domain.Command{}, fmt.Errorf("command %d: %w", id, domain.ErrNotFound)
	}
	if err != nil {
		return domain.Command{}, fmt.Errorf("store: get command %d: %w", id, err)
	}
	return row.command(), nil
}

// TransitionCommand applies a versioned state change and its log in the caller's transaction.
func (r *Repo) TransitionCommand(ctx context.Context, tx *sql.Tx, id int64, from, to domain.CommandStatus, version int, cause, eventID string) (bool, error) {
	if !slices.Contains(domain.CommandTransitions[from], to) {
		return false, fmt.Errorf("command %d: illegal transition %s -> %s", id, from, to)
	}
	res := r.session(ctx, tx).Model(&commandRow{}).Where("id = ? AND status = ? AND version = ?", id, from, version).
		Updates(map[string]any{"status": to, "version": gorm.Expr("version + 1")})
	if res.Error != nil {
		return false, fmt.Errorf("store: transition command %d: %w", id, res.Error)
	}
	if res.RowsAffected == 0 {
		return false, nil
	}
	if err := r.AppendStateLog(ctx, tx, store.EntityCommand, id, string(from), string(to), cause, eventID); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Repo) AppendStateLog(ctx context.Context, q store.DBTX, entity string, id int64, from, to, cause, eventID string) error {
	row := stateLogRow{Entity: entity, EntityID: id, FromStatus: from, ToStatus: to, Cause: cause,
		EventID: sql.NullString{String: eventID, Valid: eventID != ""}}
	if err := r.session(ctx, q).Create(&row).Error; err != nil {
		return fmt.Errorf("store: append state_log for %s %d: %w", entity, id, err)
	}
	return nil
}
