// Package store persists settlements, their items and their state log in MySQL.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/settlement/domain"
	"github.com/mafuky/invoice-hub/internal/settlement/verify"
)

// DBTX is satisfied by both *sql.DB and *sql.Tx.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// state_log.entity value.
const EntitySettlement = "settlement"

const listLimit = 200

type Repo struct {
	db *sql.DB
}

func NewRepo(db *sql.DB) *Repo { return &Repo{db: db} }

// Item is one delivered invoice inside a settlement.
type Item struct {
	ID           int64
	SettlementID int64
	InvoiceID    int64
	ProductLine  string
	Amount       money.Amount
	Verified     bool
	Checks       []verify.Check
}

// IsDuplicateKey reports whether err is a MySQL unique-key violation.
func IsDuplicateKey(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1062
}

type rowScanner interface{ Scan(dest ...any) error }

const settlementCols = `id, period, customer_id, amount, status, version, reject_reason, created_at, updated_at`

func scanSettlement(row rowScanner) (domain.Settlement, error) {
	var (
		s      domain.Settlement
		status string
	)
	err := row.Scan(&s.ID, &s.Period, &s.CustomerID, &s.Amount, &status, &s.Version, &s.RejectReason, &s.CreatedAt, &s.UpdatedAt)
	if err != nil {
		return domain.Settlement{}, err
	}
	s.Status = domain.SettlementStatus(status)
	return s, nil
}

// GetSettlement loads one settlement; it returns domain.ErrNotFound when the id is unknown.
func (r *Repo) GetSettlement(ctx context.Context, id int64) (domain.Settlement, error) {
	return r.GetSettlementTx(ctx, r.db, id)
}

func (r *Repo) GetSettlementTx(ctx context.Context, q DBTX, id int64) (domain.Settlement, error) {
	return r.getSettlement(ctx, q, id, "")
}

// GetSettlementForUpdate reads the settlement and locks its row until tx ends.
func (r *Repo) GetSettlementForUpdate(ctx context.Context, tx *sql.Tx, id int64) (domain.Settlement, error) {
	return r.getSettlement(ctx, tx, id, " FOR UPDATE")
}

func (r *Repo) getSettlement(ctx context.Context, q DBTX, id int64, lock string) (domain.Settlement, error) {
	s, err := scanSettlement(q.QueryRowContext(ctx, `SELECT `+settlementCols+` FROM settlements WHERE id = ?`+lock, id))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Settlement{}, fmt.Errorf("settlement %d: %w", id, domain.ErrNotFound)
	}
	if err != nil {
		return domain.Settlement{}, fmt.Errorf("store: get settlement %d: %w", id, err)
	}
	return s, nil
}

// ListSettlements returns the most recent settlements, newest first, up to a fixed cap.
// An empty status matches every status.
func (r *Repo) ListSettlements(ctx context.Context, status string) ([]domain.Settlement, error) {
	query := `SELECT ` + settlementCols + ` FROM settlements`
	var args []any
	if status != "" {
		query += ` WHERE status = ?`
		args = append(args, status)
	}
	query += ` ORDER BY id DESC LIMIT ?`
	args = append(args, listLimit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: list settlements: %w", err)
	}
	defer rows.Close()
	var out []domain.Settlement
	for rows.Next() {
		s, err := scanSettlement(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan settlement: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// FindOrCreateSettlement returns the settlement of customerID for period,
// creating it inside tx when it does not exist yet. A new settlement is
// written as created and moved to pending_settle at once, so it is ready for
// items; both moves are in state_log. If another transaction creates the same
// settlement first, the existing row is returned. The returned row is locked
// until tx ends, including when it already existed. This is finished code.
func (r *Repo) FindOrCreateSettlement(ctx context.Context, tx *sql.Tx, period, customerID, cause string) (domain.Settlement, error) {
	s, err := scanSettlement(tx.QueryRowContext(ctx,
		`SELECT `+settlementCols+` FROM settlements WHERE period = ? AND customer_id = ?`, period, customerID))
	if err == nil {
		// Lock by primary key only after finding a row, avoiding missing-key gap locks.
		return r.GetSettlementForUpdate(ctx, tx, s.ID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return domain.Settlement{}, fmt.Errorf("store: find settlement %s/%s: %w", period, customerID, err)
	}

	res, err := tx.ExecContext(ctx,
		`INSERT INTO settlements (period, customer_id, amount, status) VALUES (?, ?, 0, ?)`,
		period, customerID, domain.SettlementCreated)
	if IsDuplicateKey(err) {
		// A locking read sees the row the other transaction just committed.
		s, err = scanSettlement(tx.QueryRowContext(ctx,
			`SELECT `+settlementCols+` FROM settlements WHERE period = ? AND customer_id = ? FOR UPDATE`, period, customerID))
		if err != nil {
			return domain.Settlement{}, fmt.Errorf("store: re-read settlement %s/%s: %w", period, customerID, err)
		}
		return s, nil
	}
	if err != nil {
		return domain.Settlement{}, fmt.Errorf("store: insert settlement %s/%s: %w", period, customerID, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return domain.Settlement{}, err
	}
	if err := r.AppendStateLog(ctx, tx, id, "", string(domain.SettlementCreated), cause); err != nil {
		return domain.Settlement{}, err
	}
	applied, err := r.TransitionSettlement(ctx, tx, id, domain.SettlementCreated, domain.SettlementPendingSettle, 0, cause)
	if err != nil {
		return domain.Settlement{}, err
	}
	if !applied {
		return domain.Settlement{}, fmt.Errorf("settlement %d: new settlement could not be opened: %w", id, domain.ErrRefused)
	}
	return r.GetSettlementForUpdate(ctx, tx, id)
}

// FindOrCreateOpen is finished code. It returns a locked pending_settle row
// for this customer, advancing month by month past statements closed to items.
// Missing statements receive the created and pending_settle state log entries.
// The caller must keep tx open through the item insert to exclude Calculate.
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

// TransitionSettlement moves settlement id from one state to another inside
// tx with a compare-and-set on status and version, and writes one state_log
// row for the move. applied=false means the settlement was no longer in state
// from at the given version; nothing was changed. Checking that the table
// allows the move is the caller's job (CanTo).
func (r *Repo) TransitionSettlement(ctx context.Context, tx *sql.Tx, id int64, from, to domain.SettlementStatus, version int, cause string) (applied bool, err error) {
	res, err := tx.ExecContext(ctx,
		`UPDATE settlements SET status = ?, version = version + 1 WHERE id = ? AND status = ? AND version = ?`,
		to, id, from, version)
	if err != nil {
		return false, fmt.Errorf("store: transition settlement %d: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: transition settlement %d: %w", id, err)
	}
	if n == 0 {
		return false, nil
	}
	if err := r.AppendStateLog(ctx, tx, id, string(from), string(to), cause); err != nil {
		return false, err
	}
	return true, nil
}

// AppendStateLog writes one state_log row for a settlement.
func (r *Repo) AppendStateLog(ctx context.Context, q DBTX, id int64, from, to, cause string) error {
	_, err := q.ExecContext(ctx,
		`INSERT INTO state_log (entity, entity_id, from_status, to_status, cause) VALUES (?, ?, ?, ?, ?)`,
		EntitySettlement, id, from, to, cause)
	if err != nil {
		return fmt.Errorf("store: append state_log for settlement %d: %w", id, err)
	}
	return nil
}

// SetAmount stores the calculated total of a settlement.
func (r *Repo) SetAmount(ctx context.Context, q DBTX, id int64, amount money.Amount) error {
	if _, err := q.ExecContext(ctx, `UPDATE settlements SET amount = ? WHERE id = ?`, amount, id); err != nil {
		return fmt.Errorf("store: set amount of settlement %d: %w", id, err)
	}
	return nil
}

// SetRejectReason stores why a reviewer sent a settlement back.
func (r *Repo) SetRejectReason(ctx context.Context, q DBTX, id int64, reason string) error {
	if _, err := q.ExecContext(ctx, `UPDATE settlements SET reject_reason = ? WHERE id = ?`, reason, id); err != nil {
		return fmt.Errorf("store: set reject reason of settlement %d: %w", id, err)
	}
	return nil
}

// InsertItem adds a delivered invoice to a settlement. inserted=false means
// the invoice already has an item, in this or any other settlement.
func (r *Repo) InsertItem(ctx context.Context, q DBTX, it Item) (inserted bool, err error) {
	var checks any
	if it.Checks != nil {
		raw, err := json.Marshal(it.Checks)
		if err != nil {
			return false, fmt.Errorf("store: encode checks of invoice %d: %w", it.InvoiceID, err)
		}
		checks = raw
	}
	_, err = q.ExecContext(ctx,
		`INSERT INTO settlement_items (settlement_id, invoice_id, product_line, amount, verified, checks)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		it.SettlementID, it.InvoiceID, it.ProductLine, it.Amount, it.Verified, checks)
	if IsDuplicateKey(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: insert item for invoice %d: %w", it.InvoiceID, err)
	}
	return true, nil
}

// SumItems adds up the item amounts of a settlement; an empty settlement sums to 0.00.
func (r *Repo) SumItems(ctx context.Context, q DBTX, settlementID int64) (money.Amount, error) {
	var total money.Amount
	err := q.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(amount), 0) FROM settlement_items WHERE settlement_id = ?`, settlementID).Scan(&total)
	if err != nil {
		return money.Amount{}, fmt.Errorf("store: sum items of settlement %d: %w", settlementID, err)
	}
	return total, nil
}

// ListItems returns the items of a settlement, oldest first.
func (r *Repo) ListItems(ctx context.Context, settlementID int64) ([]Item, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, settlement_id, invoice_id, product_line, amount, verified, checks
		 FROM settlement_items WHERE settlement_id = ? ORDER BY id`, settlementID)
	if err != nil {
		return nil, fmt.Errorf("store: list items of settlement %d: %w", settlementID, err)
	}
	defer rows.Close()
	var out []Item
	for rows.Next() {
		var (
			it     Item
			checks sql.NullString
		)
		if err := rows.Scan(&it.ID, &it.SettlementID, &it.InvoiceID, &it.ProductLine, &it.Amount, &it.Verified, &checks); err != nil {
			return nil, fmt.Errorf("store: scan item: %w", err)
		}
		if checks.Valid && checks.String != "" {
			if err := json.Unmarshal([]byte(checks.String), &it.Checks); err != nil {
				return nil, fmt.Errorf("store: decode checks of invoice %d: %w", it.InvoiceID, err)
			}
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// HasItem checks whether an invoice already belongs to a statement before re-verifying it.
func (r *Repo) HasItem(ctx context.Context, q DBTX, invoiceID int64) (bool, error) {
	var exists bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM settlement_items WHERE invoice_id = ?)`, invoiceID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("store: find item for invoice %d: %w", invoiceID, err)
	}
	return exists, nil
}
