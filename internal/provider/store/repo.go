// Package store persists provider orders in MySQL and raw supplier callbacks in MongoDB.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// provider_orders.status values.
const (
	StatusSubmitted = "submitted"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
	StatusTimeout   = "timeout"
)

var ErrNotFound = errors.New("provider order not found")

// DBTX is satisfied by both *sql.DB and *sql.Tx.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Order is one supplier order placed for a task.
type Order struct {
	ID              int64
	TaskID          int64
	InvoiceID       int64
	Supplier        string
	SupplierOrderNo string
	Status          string
	Outcome         string
	InvoiceNo       string
	Version         int
}

// NewOrder is a supplier order the supplier has just accepted.
type NewOrder struct {
	TaskID          int64
	InvoiceID       int64
	Supplier        string
	SupplierOrderNo string
}

type Repo struct {
	db *sql.DB
}

func NewRepo(db *sql.DB) *Repo { return &Repo{db: db} }

const orderCols = `id, task_id, invoice_id, supplier, supplier_order_no, status, outcome, invoice_no, version`

type rowScanner interface{ Scan(dest ...any) error }

func scanOrder(row rowScanner) (Order, error) {
	var (
		o         Order
		invoiceNo sql.NullString
	)
	err := row.Scan(&o.ID, &o.TaskID, &o.InvoiceID, &o.Supplier, &o.SupplierOrderNo, &o.Status, &o.Outcome, &invoiceNo, &o.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return Order{}, ErrNotFound
	}
	if err != nil {
		return Order{}, err
	}
	o.InvoiceNo = invoiceNo.String
	return o, nil
}

// InsertOrder records a submitted order. A second insert for the same task
// reports inserted=false and leaves the first row alone.
func (r *Repo) InsertOrder(ctx context.Context, q DBTX, o NewOrder) (inserted bool, err error) {
	_, err = q.ExecContext(ctx,
		`INSERT INTO provider_orders (task_id, invoice_id, supplier, supplier_order_no, status) VALUES (?, ?, ?, ?, ?)`,
		o.TaskID, o.InvoiceID, o.Supplier, o.SupplierOrderNo, StatusSubmitted)
	if err != nil {
		if isDuplicateTask(err) {
			return false, nil
		}
		return false, fmt.Errorf("insert provider order for task %d: %w", o.TaskID, err)
	}
	return true, nil
}

func isDuplicateTask(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1062 && strings.Contains(me.Message, "uk_provider_orders_task")
}

// OrderByTask reads the order of a task outside any transaction.
func (r *Repo) OrderByTask(ctx context.Context, taskID int64) (Order, error) {
	return r.GetByTask(ctx, r.db, taskID)
}

func (r *Repo) GetByTask(ctx context.Context, q DBTX, taskID int64) (Order, error) {
	o, err := scanOrder(q.QueryRowContext(ctx, `SELECT `+orderCols+` FROM provider_orders WHERE task_id = ?`, taskID))
	if err != nil {
		return Order{}, fmt.Errorf("order of task %d: %w", taskID, err)
	}
	return o, nil
}

func (r *Repo) GetBySupplierOrder(ctx context.Context, q DBTX, supplier, supplierOrderNo string) (Order, error) {
	return r.getBySupplierOrder(ctx, q, supplier, supplierOrderNo, "")
}

// GetBySupplierOrderForUpdate locks the row until tx ends.
func (r *Repo) GetBySupplierOrderForUpdate(ctx context.Context, tx *sql.Tx, supplier, supplierOrderNo string) (Order, error) {
	return r.getBySupplierOrder(ctx, tx, supplier, supplierOrderNo, " FOR UPDATE")
}

func (r *Repo) getBySupplierOrder(ctx context.Context, q DBTX, supplier, supplierOrderNo, lock string) (Order, error) {
	o, err := scanOrder(q.QueryRowContext(ctx,
		`SELECT `+orderCols+` FROM provider_orders WHERE supplier = ? AND supplier_order_no = ?`+lock, supplier, supplierOrderNo))
	if err != nil {
		return Order{}, fmt.Errorf("order %s/%s: %w", supplier, supplierOrderNo, err)
	}
	return o, nil
}

// ExcludedSuppliers lists the suppliers that must not receive another order
// for the invoice: those whose order failed or timed out, and those still
// holding a submitted order of a task older than taskID.
func (r *Repo) ExcludedSuppliers(ctx context.Context, q DBTX, invoiceID, taskID int64) ([]string, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT DISTINCT supplier FROM provider_orders
		 WHERE invoice_id = ? AND task_id <> ?
		   AND (status IN (?, ?) OR (status = ? AND task_id < ?))
		 ORDER BY supplier`,
		invoiceID, taskID, StatusFailed, StatusTimeout, StatusSubmitted, taskID)
	if err != nil {
		return nil, fmt.Errorf("excluded suppliers of invoice %d: %w", invoiceID, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// SetOutcome moves an order to a final status. It reports false when the
// order is no longer at the given version.
func (r *Repo) SetOutcome(ctx context.Context, tx *sql.Tx, id int64, version int, status, outcome, invoiceNo string) (bool, error) {
	var inv any
	if invoiceNo != "" {
		inv = invoiceNo
	}
	res, err := tx.ExecContext(ctx,
		`UPDATE provider_orders SET status = ?, outcome = ?, invoice_no = ?, version = version + 1 WHERE id = ? AND version = ?`,
		status, outcome, inv, id, version)
	if err != nil {
		return false, fmt.Errorf("update provider order %d: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}
