package gormstore

import (
	"context"
	"database/sql"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"gorm.io/gorm/clause"
)

// ListCommandsForUpdate reads an invoice's commands in id order and locks them until tx ends.
func (r *Repo) ListCommandsForUpdate(ctx context.Context, tx *sql.Tx, invoiceID int64) ([]domain.Command, error) {
	var rows []commandRow
	if err := r.session(ctx, tx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("invoice_id = ?", invoiceID).Order("id").Find(&rows).Error; err != nil {
		return nil, err
	}
	var out []domain.Command
	for _, row := range rows {
		out = append(out, row.command())
	}
	return out, nil
}

// ListTasksForUpdate reads a command's attempts in id order and locks them until tx ends.
func (r *Repo) ListTasksForUpdate(ctx context.Context, tx *sql.Tx, commandID int64) ([]domain.Task, error) {
	var rows []taskRow
	if err := r.session(ctx, tx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("command_id = ?", commandID).Order("id").Find(&rows).Error; err != nil {
		return nil, err
	}
	var out []domain.Task
	for _, row := range rows {
		out = append(out, row.task())
	}
	return out, nil
}
