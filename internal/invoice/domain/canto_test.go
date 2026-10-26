//go:build lesson

package domain

import "testing"

func TestCanTo_InvoiceTable(t *testing.T) {
	cases := []struct {
		from, to InvoiceStatus
		want     bool
	}{
		{InvoiceApplying, InvoiceIssuing, true},
		{InvoiceIssuing, InvoiceIssued, true},
		{InvoiceIssuing, InvoiceFailed, true},
		{InvoiceIssued, InvoiceDelivered, true},
		{InvoiceIssued, InvoiceRedFlushing, false},
		{InvoiceClosed, InvoiceRedFlushing, true},
		{InvoiceDelivered, InvoiceClosed, true},
		{InvoiceRedFlushing, InvoiceRedFlushed, true},
		{InvoiceDelivered, InvoiceIssuing, false},
		{InvoiceApplying, InvoiceDelivered, false},
		{InvoiceIssuing, InvoiceDelivered, false},
		{InvoiceClosed, InvoiceApplying, false},
		{InvoiceClosed, InvoiceIssued, false},
		{InvoiceRedFlushed, InvoiceIssued, false},
		{InvoiceFailed, InvoiceIssuing, false},
		{InvoiceIssued, InvoiceIssued, false},
		{InvoiceClosed, InvoiceClosed, false},
	}
	for _, c := range cases {
		if got := c.from.CanTo(c.to); got != c.want {
			t.Errorf("invoice %s -> %s: got %v, want %v", c.from, c.to, got, c.want)
		}
	}
	for _, from := range []InvoiceStatus{InvoiceApplying, InvoiceIssuing, InvoiceIssued, InvoiceDelivered, InvoiceClosed, InvoiceRedFlushing, InvoiceRedFlushed, InvoiceFailed} {
		if InvoiceRedFlushed.CanTo(from) {
			t.Errorf("red flushed invoice must not move to %s", from)
		}
	}
}

func TestCanTo_TaskTable(t *testing.T) {
	cases := []struct {
		from, to TaskStatus
		want     bool
	}{
		{TaskPending, TaskRunning, true},
		{TaskRunning, TaskWaitingCallback, true},
		{TaskWaitingCallback, TaskDone, true},
		{TaskWaitingCallback, TaskFailed, true},
		{TaskDone, TaskRunning, false},
		{TaskDone, TaskFailed, false},
		{TaskFailed, TaskDone, false},
		{TaskPending, TaskDone, false},
		{TaskRunning, TaskRunning, false},
	}
	for _, c := range cases {
		if got := c.from.CanTo(c.to); got != c.want {
			t.Errorf("task %s -> %s: got %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

func TestCanTo_CommandTable(t *testing.T) {
	cases := []struct {
		from, to CommandStatus
		want     bool
	}{
		{CommandCreated, CommandDispatched, true},
		{CommandDispatched, CommandSucceeded, true},
		{CommandDispatched, CommandFailed, true},
		{CommandCreated, CommandSucceeded, false},
		{CommandFailed, CommandDispatched, false},
		{CommandSucceeded, CommandFailed, false},
		{CommandCreated, CommandCreated, false},
	}
	for _, c := range cases {
		if got := c.from.CanTo(c.to); got != c.want {
			t.Errorf("command %s -> %s: got %v, want %v", c.from, c.to, got, c.want)
		}
	}
}
