// Package domain holds the invoice, command and task states and the allowed moves between them.
package domain

import "errors"

// ErrNotFound is returned when an invoice, command or task does not exist.
var ErrNotFound = errors.New("not found")

type InvoiceStatus string

const (
	InvoiceApplying    InvoiceStatus = "applying"
	InvoiceIssuing     InvoiceStatus = "issuing"
	InvoiceIssued      InvoiceStatus = "issued"
	InvoiceDelivered   InvoiceStatus = "delivered"
	InvoiceClosed      InvoiceStatus = "closed"
	InvoiceRedFlushing InvoiceStatus = "red_flushing"
	InvoiceRedFlushed  InvoiceStatus = "red_flushed"
	InvoiceFailed      InvoiceStatus = "failed"
)

type CommandStatus string

const (
	CommandCreated    CommandStatus = "created"
	CommandDispatched CommandStatus = "dispatched"
	CommandSucceeded  CommandStatus = "succeeded"
	CommandFailed     CommandStatus = "failed"
)

type TaskStatus string

const (
	TaskPending         TaskStatus = "pending"
	TaskRunning         TaskStatus = "running"
	TaskWaitingCallback TaskStatus = "waiting_callback"
	TaskDone            TaskStatus = "done"
	TaskFailed          TaskStatus = "failed"
)

// Command kinds as stored in commands.kind.
const (
	KindIssue      = "ISSUE"
	KindRedFlush   = "RED_FLUSH"
	KindReissue    = "REISSUE"
	KindSupplement = "SUPPLEMENT"
)

var InvoiceTransitions = map[InvoiceStatus][]InvoiceStatus{
	InvoiceApplying:    {InvoiceIssuing},
	InvoiceIssuing:     {InvoiceIssued, InvoiceFailed},
	InvoiceIssued:      {InvoiceDelivered, InvoiceRedFlushing},
	InvoiceDelivered:   {InvoiceClosed},
	InvoiceRedFlushing: {InvoiceRedFlushed},
}

var CommandTransitions = map[CommandStatus][]CommandStatus{
	CommandCreated:    {CommandDispatched},
	CommandDispatched: {CommandSucceeded, CommandFailed},
}

var TaskTransitions = map[TaskStatus][]TaskStatus{
	TaskPending:         {TaskRunning},
	TaskRunning:         {TaskWaitingCallback},
	TaskWaitingCallback: {TaskDone, TaskFailed},
}

// KnownInvoiceStatus reports whether s is one of the invoice states.
func KnownInvoiceStatus(s InvoiceStatus) bool {
	switch s {
	case InvoiceApplying, InvoiceIssuing, InvoiceIssued, InvoiceDelivered,
		InvoiceClosed, InvoiceRedFlushing, InvoiceRedFlushed, InvoiceFailed:
		return true
	}
	return false
}
