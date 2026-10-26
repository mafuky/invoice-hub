package gormstore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"database/sql"
	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/mafuky/invoice-hub/internal/platform/gormx"
	"github.com/mafuky/invoice-hub/internal/provider/store"
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

func (r *Repo) InsertOrder(ctx context.Context, q store.DBTX, o store.NewOrder) (bool, error) {
	row := orderRow{TaskID: o.TaskID, InvoiceID: o.InvoiceID, Supplier: o.Supplier, SupplierOrderNo: o.SupplierOrderNo, Status: store.StatusSubmitted}
	if err := r.session(ctx, q).Create(&row).Error; err != nil {
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == 1062 && strings.Contains(me.Message, "uk_provider_orders_task") {
			return false, nil
		}
		return false, fmt.Errorf("insert provider order for task %d: %w", o.TaskID, err)
	}
	return true, nil
}

func (r *Repo) OrderByTask(ctx context.Context, taskID int64) (store.Order, error) {
	return r.GetByTask(ctx, r.db, taskID)
}

func readOrder(db *gorm.DB) (store.Order, error) {
	var row orderRow
	err := db.Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return store.Order{}, store.ErrNotFound
	}
	if err != nil {
		return store.Order{}, fmt.Errorf("read provider order: %w", err)
	}
	return row.order(), nil
}

func (r *Repo) GetByTask(ctx context.Context, q store.DBTX, taskID int64) (store.Order, error) {
	return readOrder(r.session(ctx, q).Where("task_id = ?", taskID))
}

func (r *Repo) GetBySupplierOrder(ctx context.Context, q store.DBTX, supplier, supplierOrderNo string) (store.Order, error) {
	return readOrder(r.session(ctx, q).Where("supplier = ? AND supplier_order_no = ?", supplier, supplierOrderNo))
}

func (r *Repo) GetBySupplierOrderForUpdate(ctx context.Context, tx *sql.Tx, supplier, supplierOrderNo string) (store.Order, error) {
	return readOrder(r.session(ctx, tx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("supplier = ? AND supplier_order_no = ?", supplier, supplierOrderNo))
}

func (r *Repo) ExcludedSuppliers(ctx context.Context, q store.DBTX, invoiceID, taskID int64) ([]string, error) {
	var out []string
	err := r.session(ctx, q).Model(&orderRow{}).Distinct("supplier").
		Where("invoice_id = ? AND task_id <> ? AND (status IN (?, ?) OR (status = ? AND task_id < ?))", invoiceID, taskID, store.StatusFailed, store.StatusTimeout, store.StatusSubmitted, taskID).
		Order("supplier").Pluck("supplier", &out).Error
	if err != nil {
		return nil, fmt.Errorf("excluded suppliers of invoice %d: %w", invoiceID, err)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func (r *Repo) ApplyOutcome(ctx context.Context, tx *sql.Tx, orderID int64, version int, seq int64, status, outcome, invoiceNo string) (bool, error) {
	var inv any
	if invoiceNo != "" {
		inv = invoiceNo
	}
	res := r.session(ctx, tx).Model(&orderRow{}).
		Where("id = ? AND version = ? AND last_event_seq < ? AND status <> ?", orderID, version, seq, store.StatusSucceeded).
		Updates(map[string]any{"status": status, "outcome": outcome, "invoice_no": inv, "last_event_seq": seq, "version": gorm.Expr("version + 1")})
	if res.Error != nil {
		return false, fmt.Errorf("update provider order %d: %w", orderID, res.Error)
	}
	return res.RowsAffected == 1, nil
}
