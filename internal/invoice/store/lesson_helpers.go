package store

import (
	"context"
	"database/sql"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
)

// ListCommandsForUpdate reads an invoice's commands in id order and locks them until tx ends.
func (r *Repo) ListCommandsForUpdate(ctx context.Context, tx *sql.Tx, invoiceID int64) ([]domain.Command, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id, invoice_id, kind, status, reason, version FROM commands WHERE invoice_id=? ORDER BY id FOR UPDATE`, invoiceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Command
	for rows.Next() {
		var c domain.Command
		if err := rows.Scan(&c.ID, &c.InvoiceID, &c.Kind, &c.Status, &c.Reason, &c.Version); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ListTasksForUpdate reads a command's attempts in id order and locks them until tx ends.
func (r *Repo) ListTasksForUpdate(ctx context.Context, tx *sql.Tx, commandID int64) ([]domain.Task, error) {
	rows, err := tx.QueryContext(ctx, `SELECT `+taskCols+` FROM tasks WHERE command_id=? ORDER BY id FOR UPDATE`, commandID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Task
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, task)
	}
	return out, rows.Err()
}
