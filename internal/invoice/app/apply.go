// Implemented in lesson 2.

// Package app holds the invoice service's use cases: accepting issue requests,
// advancing invoices from supplier and delivery events, and recovering stuck tasks.
package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mafuky/invoice-hub/internal/invoice/store"
	"github.com/mafuky/invoice-hub/internal/platform/money"
)

// ApplyInput is one issue request from a business system.
type ApplyInput struct {
	BizOrderNo  string
	ProductLine string
	Title       string
	TaxNo       string
	Amount      money.Amount
	CustomerID  string
}

// Service implements the invoice commands.
type Service struct {
	db   *sql.DB
	repo *store.Repo
}

func NewService(db *sql.DB, repo *store.Repo) *Service {
	return &Service{db: db, repo: repo}
}

// Apply accepts an issue request: it creates the invoice (applying), its ISSUE
// command, a pending task and the "task created" outbox event in one
// transaction. When the biz_order_no already has an invoice it returns that
// invoice with existed=true and creates nothing.
//
// Invariant: one biz_order_no has exactly one invoice, however many times and
// however concurrently the request arrives, and an invoice never exists
// without its task and its outbox event.
//
// A retry after a network timeout that opens a second invoice makes the
// enterprise pay tax twice for one trip; an invoice saved without its event
// stays in applying forever and no supplier is ever asked to issue it.
func (s *Service) Apply(ctx context.Context, in ApplyInput) (invoiceID int64, existed bool, err error) {
	return 0, false, fmt.Errorf("lesson 2: %w", errors.ErrUnsupported)
}
