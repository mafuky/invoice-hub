package store

import (
	"context"
	"database/sql"
	"time"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
)

// Store is the persistence boundary used by invoice application and gRPC handlers.
type Store interface {
	GetInvoiceByBizOrderNo(context.Context, DBTX, string) (domain.Invoice, error)
	InsertInvoice(context.Context, DBTX, NewInvoice) (int64, error)
	InsertCommand(context.Context, DBTX, int64, string, string) (int64, error)
	InsertTask(context.Context, DBTX, int64, int64, string) (int64, error)
	GetTask(context.Context, DBTX, int64) (domain.Task, error)
	GetCommand(context.Context, DBTX, int64) (domain.Command, error)
	ListTimedOutTasks(context.Context, time.Time, int) ([]domain.Task, error)
	ListCommandsForUpdate(context.Context, *sql.Tx, int64) ([]domain.Command, error)
	ListTasksForUpdate(context.Context, *sql.Tx, int64) ([]domain.Task, error)
	GetInvoice(context.Context, int64) (domain.Invoice, error)
	GetInvoiceTx(context.Context, DBTX, int64) (domain.Invoice, error)
	GetInvoiceForUpdate(context.Context, *sql.Tx, int64) (domain.Invoice, error)
	ListInvoices(context.Context, string, int64, int) ([]domain.Invoice, error)
	GetTaskForUpdate(context.Context, *sql.Tx, int64) (domain.Task, error)
	GetCommandForUpdate(context.Context, *sql.Tx, int64) (domain.Command, error)
	SetInvoiceNo(context.Context, DBTX, int64, string) error
	SetTaskDeadline(context.Context, DBTX, int64, time.Time) error
	TransitionInvoice(context.Context, *sql.Tx, int64, domain.InvoiceStatus, domain.InvoiceStatus, int, string, string) (bool, error)
	TransitionTask(context.Context, *sql.Tx, int64, domain.TaskStatus, domain.TaskStatus, int, string, string) (bool, error)
	TransitionCommand(context.Context, *sql.Tx, int64, domain.CommandStatus, domain.CommandStatus, int, string, string) (bool, error)
	AppendStateLog(context.Context, DBTX, string, int64, string, string, string, string) error
}

var _ Store = (*Repo)(nil)
