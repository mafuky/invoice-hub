// Package store persists invoices, commands, tasks and their state log in MySQL.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/platform/money"
)

// DBTX is satisfied by both *sql.DB and *sql.Tx.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// state_log.entity values.
const (
	EntityInvoice = "invoice"
	EntityTask    = "task"
)

type Repo struct {
	db *sql.DB
}

func NewRepo(db *sql.DB) *Repo { return &Repo{db: db} }

// NewInvoice is the data of an issue request, before it has an id.
type NewInvoice struct {
	BizOrderNo  string
	ProductLine string
	Title       string
	TaxNo       string
	Amount      money.Amount
	CustomerID  string
}

const invoiceCols = `id, biz_order_no, product_line, title, tax_no, amount, customer_id, status, invoice_no, version, created_at, updated_at`

type rowScanner interface{ Scan(dest ...any) error }

func scanInvoice(row rowScanner) (domain.Invoice, error) {
	var (
		inv       domain.Invoice
		status    string
		invoiceNo sql.NullString
	)
	err := row.Scan(&inv.ID, &inv.BizOrderNo, &inv.ProductLine, &inv.Title, &inv.TaxNo, &inv.Amount,
		&inv.CustomerID, &status, &invoiceNo, &inv.Version, &inv.CreatedAt, &inv.UpdatedAt)
	if err != nil {
		return domain.Invoice{}, err
	}
	inv.Status = domain.InvoiceStatus(status)
	inv.InvoiceNo = invoiceNo.String
	return inv, nil
}

// GetInvoice loads one invoice; it returns domain.ErrNotFound when the id is unknown.
func (r *Repo) GetInvoice(ctx context.Context, id int64) (domain.Invoice, error) {
	return r.GetInvoiceTx(ctx, r.db, id)
}

func (r *Repo) GetInvoiceTx(ctx context.Context, q DBTX, id int64) (domain.Invoice, error) {
	return r.getInvoice(ctx, q, id, "")
}

// GetInvoiceForUpdate reads the invoice and locks its row until tx ends.
func (r *Repo) GetInvoiceForUpdate(ctx context.Context, tx *sql.Tx, id int64) (domain.Invoice, error) {
	return r.getInvoice(ctx, tx, id, " FOR UPDATE")
}

func (r *Repo) getInvoice(ctx context.Context, q DBTX, id int64, lock string) (domain.Invoice, error) {
	inv, err := scanInvoice(q.QueryRowContext(ctx, `SELECT `+invoiceCols+` FROM invoices WHERE id = ?`+lock, id))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Invoice{}, fmt.Errorf("invoice %d: %w", id, domain.ErrNotFound)
	}
	if err != nil {
		return domain.Invoice{}, fmt.Errorf("store: get invoice %d: %w", id, err)
	}
	return inv, nil
}

// GetInvoiceByBizOrderNo finds the invoice already created for a business order.
func (r *Repo) GetInvoiceByBizOrderNo(ctx context.Context, q DBTX, bizOrderNo string) (domain.Invoice, error) {
	inv, err := scanInvoice(q.QueryRowContext(ctx, `SELECT `+invoiceCols+` FROM invoices WHERE biz_order_no = ?`, bizOrderNo))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Invoice{}, fmt.Errorf("invoice for %s: %w", bizOrderNo, domain.ErrNotFound)
	}
	if err != nil {
		return domain.Invoice{}, fmt.Errorf("store: get invoice by biz_order_no: %w", err)
	}
	return inv, nil
}

// ListInvoices returns up to limit invoices with id greater than afterID, oldest first.
// An empty status matches every status.
func (r *Repo) ListInvoices(ctx context.Context, status string, afterID int64, limit int) ([]domain.Invoice, error) {
	query := `SELECT ` + invoiceCols + ` FROM invoices WHERE id > ?`
	args := []any{afterID}
	if status != "" {
		query += ` AND status = ?`
		args = append(args, status)
	}
	query += ` ORDER BY id LIMIT ?`
	args = append(args, limit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: list invoices: %w", err)
	}
	defer rows.Close()
	var out []domain.Invoice
	for rows.Next() {
		inv, err := scanInvoice(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan invoice: %w", err)
		}
		out = append(out, inv)
	}
	return out, rows.Err()
}

// IsDuplicateKey reports whether err is a MySQL unique-key violation.
func IsDuplicateKey(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1062
}

// InsertInvoice creates an invoice in status applying.
func (r *Repo) InsertInvoice(ctx context.Context, q DBTX, in NewInvoice) (int64, error) {
	res, err := q.ExecContext(ctx,
		`INSERT INTO invoices (biz_order_no, product_line, title, tax_no, amount, customer_id, status)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		in.BizOrderNo, in.ProductLine, in.Title, in.TaxNo, in.Amount, in.CustomerID, domain.InvoiceApplying)
	if err != nil {
		return 0, fmt.Errorf("store: insert invoice %s: %w", in.BizOrderNo, err)
	}
	return res.LastInsertId()
}

func (r *Repo) InsertCommand(ctx context.Context, q DBTX, invoiceID int64, kind, reason string) (int64, error) {
	res, err := q.ExecContext(ctx,
		`INSERT INTO commands (invoice_id, kind, status, reason) VALUES (?, ?, ?, ?)`,
		invoiceID, kind, domain.CommandCreated, reason)
	if err != nil {
		return 0, fmt.Errorf("store: insert command for invoice %d: %w", invoiceID, err)
	}
	return res.LastInsertId()
}

// InsertTask creates a pending task; an empty supplier leaves the choice to provider-svc.
func (r *Repo) InsertTask(ctx context.Context, q DBTX, commandID, invoiceID int64, supplier string) (int64, error) {
	var sup any
	if supplier != "" {
		sup = supplier
	}
	res, err := q.ExecContext(ctx,
		`INSERT INTO tasks (command_id, invoice_id, supplier, status) VALUES (?, ?, ?, ?)`,
		commandID, invoiceID, sup, domain.TaskPending)
	if err != nil {
		return 0, fmt.Errorf("store: insert task for invoice %d: %w", invoiceID, err)
	}
	return res.LastInsertId()
}

const taskCols = `id, command_id, invoice_id, supplier, status, attempt, deadline_at, version`

func scanTask(row rowScanner) (domain.Task, error) {
	var (
		t        domain.Task
		supplier sql.NullString
		status   string
		deadline sql.NullTime
	)
	if err := row.Scan(&t.ID, &t.CommandID, &t.InvoiceID, &supplier, &status, &t.Attempt, &deadline, &t.Version); err != nil {
		return domain.Task{}, err
	}
	t.Supplier = supplier.String
	t.Status = domain.TaskStatus(status)
	if deadline.Valid {
		d := deadline.Time
		t.DeadlineAt = &d
	}
	return t, nil
}

func (r *Repo) GetTask(ctx context.Context, q DBTX, id int64) (domain.Task, error) {
	return r.getTask(ctx, q, id, "")
}

// GetTaskForUpdate reads the task and locks its row until tx ends.
func (r *Repo) GetTaskForUpdate(ctx context.Context, tx *sql.Tx, id int64) (domain.Task, error) {
	return r.getTask(ctx, tx, id, " FOR UPDATE")
}

func (r *Repo) getTask(ctx context.Context, q DBTX, id int64, lock string) (domain.Task, error) {
	t, err := scanTask(q.QueryRowContext(ctx, `SELECT `+taskCols+` FROM tasks WHERE id = ?`+lock, id))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Task{}, fmt.Errorf("task %d: %w", id, domain.ErrNotFound)
	}
	if err != nil {
		return domain.Task{}, fmt.Errorf("store: get task %d: %w", id, err)
	}
	return t, nil
}

// SetInvoiceNo records the number the supplier issued.
func (r *Repo) SetInvoiceNo(ctx context.Context, q DBTX, id int64, invoiceNo string) error {
	res, err := q.ExecContext(ctx, `UPDATE invoices SET invoice_no = ? WHERE id = ?`, invoiceNo, id)
	if err != nil {
		return fmt.Errorf("store: set invoice_no on %d: %w", id, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("invoice %d: %w", id, domain.ErrNotFound)
	}
	return nil
}

// SetTaskDeadline sets the time by which the supplier callback must have arrived.
func (r *Repo) SetTaskDeadline(ctx context.Context, q DBTX, id int64, at time.Time) error {
	_, err := q.ExecContext(ctx, `UPDATE tasks SET deadline_at = ? WHERE id = ?`, at.UTC(), id)
	if err != nil {
		return fmt.Errorf("store: set deadline on task %d: %w", id, err)
	}
	return nil
}

// ListTimedOutTasks returns tasks still waiting for a callback after their deadline.
func (r *Repo) ListTimedOutTasks(ctx context.Context, now time.Time, limit int) ([]domain.Task, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT `+taskCols+` FROM tasks WHERE status = ? AND deadline_at <= ? ORDER BY deadline_at LIMIT ?`,
		domain.TaskWaitingCallback, now.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("store: list timed-out tasks: %w", err)
	}
	defer rows.Close()
	var out []domain.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan task: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// GetCommand loads one command; it returns domain.ErrNotFound when the id is unknown.
func (r *Repo) GetCommand(ctx context.Context, q DBTX, id int64) (domain.Command, error) {
	var (
		c      domain.Command
		status string
	)
	err := q.QueryRowContext(ctx, `SELECT id, invoice_id, kind, status, reason FROM commands WHERE id = ?`, id).
		Scan(&c.ID, &c.InvoiceID, &c.Kind, &status, &c.Reason)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Command{}, fmt.Errorf("command %d: %w", id, domain.ErrNotFound)
	}
	if err != nil {
		return domain.Command{}, fmt.Errorf("store: get command %d: %w", id, err)
	}
	c.Status = domain.CommandStatus(status)
	return c, nil
}

// AppendStateLog writes a state_log row without changing any entity, for
// events that are noted rather than applied.
func (r *Repo) AppendStateLog(ctx context.Context, q DBTX, entity string, id int64, from, to, cause, eventID string) error {
	var ev any
	if eventID != "" {
		ev = eventID
	}
	_, err := q.ExecContext(ctx,
		`INSERT INTO state_log (entity, entity_id, from_status, to_status, cause, event_id) VALUES (?, ?, ?, ?, ?, ?)`,
		entity, id, from, to, cause, ev)
	if err != nil {
		return fmt.Errorf("store: append state_log for %s %d: %w", entity, id, err)
	}
	return nil
}
