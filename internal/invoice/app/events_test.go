package app

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
)

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestEvents_MapsRoutingKeysToTransitions(t *testing.T) {
	outcome := event.TaskOutcome{TaskID: 11, InvoiceID: 7, Supplier: "air-a", SupplierOrderNo: "AIR-A-ORD-0001", InvoiceNo: "AIR-A-12345678"}
	failed := event.TaskOutcome{TaskID: 11, InvoiceID: 7, Supplier: "air-a", SupplierOrderNo: "AIR-A-ORD-0001", Reason: "supplier rejected title"}
	submitted := event.TaskSubmitted{TaskID: 11, InvoiceID: 7, Supplier: "air-a", SupplierOrderNo: "AIR-A-ORD-0001"}
	toWaiting := []TaskStep{{domain.TaskPending, domain.TaskRunning}, {domain.TaskRunning, domain.TaskWaitingCallback}}

	cases := []struct {
		name   string
		key    string
		body   any
		status domain.InvoiceStatus
		kind   string
		task   domain.TaskStatus // zero means waiting_callback
		want   Plan
	}{
		{
			"supplier accepted the first order",
			mq.ProviderTaskSubmitted, submitted, domain.InvoiceApplying, domain.KindIssue, "",
			Plan{
				TaskID: 11, InvoiceID: 7, TaskSteps: toWaiting, SetDeadline: true,
				InvoiceFrom: domain.InvoiceApplying, InvoiceTo: domain.InvoiceIssuing,
			},
		},
		{
			"reopened task on a new supplier leaves the issuing invoice alone",
			mq.ProviderTaskSubmitted, submitted, domain.InvoiceIssuing, domain.KindIssue, "",
			Plan{TaskID: 11, InvoiceID: 7, TaskSteps: toWaiting, SetDeadline: true},
		},
		{
			"supplement order accepted",
			mq.ProviderTaskSubmitted, submitted, domain.InvoiceApplying, domain.KindSupplement, "",
			Plan{
				TaskID: 11, InvoiceID: 7, TaskSteps: toWaiting, SetDeadline: true,
				InvoiceFrom: domain.InvoiceApplying, InvoiceTo: domain.InvoiceIssuing,
			},
		},
		{
			"supplier callback succeeded",
			mq.ProviderTaskSucceeded, outcome, domain.InvoiceIssuing, domain.KindIssue, "",
			Plan{
				TaskID: 11, InvoiceID: 7,
				TaskSteps:   []TaskStep{{domain.TaskWaitingCallback, domain.TaskDone}},
				InvoiceFrom: domain.InvoiceIssuing, InvoiceTo: domain.InvoiceIssued,
				InvoiceNo: "AIR-A-12345678", Emit: mq.InvoiceIssued,
			},
		},
		{
			"supplier callback failed: the invoice stays issuing and a replacement task is opened",
			mq.ProviderTaskFailed, failed, domain.InvoiceIssuing, domain.KindIssue, "",
			Plan{TaskID: 11, InvoiceID: 7, TaskSteps: []TaskStep{{domain.TaskWaitingCallback, domain.TaskFailed}}, Reopen: true},
		},
		{
			"red flush order accepted",
			mq.ProviderTaskSubmitted, submitted, domain.InvoiceClosed, domain.KindRedFlush, "",
			Plan{
				TaskID: 11, InvoiceID: 7, TaskSteps: toWaiting, SetDeadline: true,
				InvoiceFrom: domain.InvoiceClosed, InvoiceTo: domain.InvoiceRedFlushing,
			},
		},
		{
			"red flush succeeded issues no new invoice event",
			mq.ProviderTaskSucceeded, event.TaskOutcome{TaskID: 11, InvoiceID: 7, Supplier: "air-a", InvoiceNo: "AIR-A-R-0001"},
			domain.InvoiceRedFlushing, domain.KindRedFlush, "",
			Plan{
				TaskID: 11, InvoiceID: 7,
				TaskSteps:   []TaskStep{{domain.TaskWaitingCallback, domain.TaskDone}},
				InvoiceFrom: domain.InvoiceRedFlushing, InvoiceTo: domain.InvoiceRedFlushed,
			},
		},
		{
			"red flush failed keeps the invoice and calls an operator",
			mq.ProviderTaskFailed, failed, domain.InvoiceRedFlushing, domain.KindRedFlush, "",
			Plan{
				TaskID: 11, InvoiceID: 7,
				TaskSteps:     []TaskStep{{domain.TaskWaitingCallback, domain.TaskFailed}},
				NeedsOperator: true,
			},
		},
		{
			"reissue order accepted takes the red flush path",
			mq.ProviderTaskSubmitted, submitted, domain.InvoiceClosed, domain.KindReissue, "",
			Plan{
				TaskID: 11, InvoiceID: 7, TaskSteps: toWaiting, SetDeadline: true,
				InvoiceFrom: domain.InvoiceClosed, InvoiceTo: domain.InvoiceRedFlushing,
			},
		},
		{
			"reissue red flush succeeded asks for the replacement invoice",
			mq.ProviderTaskSucceeded, event.TaskOutcome{TaskID: 11, InvoiceID: 7, Supplier: "air-a", InvoiceNo: "AIR-A-R-0002"},
			domain.InvoiceRedFlushing, domain.KindReissue, "",
			Plan{
				TaskID: 11, InvoiceID: 7,
				TaskSteps:   []TaskStep{{domain.TaskWaitingCallback, domain.TaskDone}},
				InvoiceFrom: domain.InvoiceRedFlushing, InvoiceTo: domain.InvoiceRedFlushed,
				IssueReplacement: true,
			},
		},
		{
			"duplicate success callback on a done task is acknowledged",
			mq.ProviderTaskSucceeded, outcome, domain.InvoiceIssued, domain.KindIssue, domain.TaskDone,
			Plan{TaskID: 11, InvoiceID: 7, Ack: true},
		},
		{
			"late failure callback on a done task is acknowledged",
			mq.ProviderTaskFailed, failed, domain.InvoiceIssued, domain.KindIssue, domain.TaskDone,
			Plan{TaskID: 11, InvoiceID: 7, Ack: true},
		},
		{
			"late success after the task was given up is flagged as a possible double issue",
			mq.ProviderTaskSucceeded, outcome, domain.InvoiceIssuing, domain.KindIssue, domain.TaskFailed,
			Plan{TaskID: 11, InvoiceID: 7, Flag: true},
		},
		{
			"customer push and finance write-back done",
			mq.DeliverySucceeded, event.DeliveryDone{InvoiceID: 7, InvoiceVersion: 3}, domain.InvoiceIssued, "", "",
			Plan{InvoiceID: 7, InvoiceFrom: domain.InvoiceIssued, InvoiceTo: domain.InvoiceDelivered, Emit: mq.InvoiceDelivered},
		},
		{
			"delivery gave up stays issued for an operator",
			mq.DeliveryGaveUp, event.DeliveryDone{InvoiceID: 7, InvoiceVersion: 3}, domain.InvoiceIssued, "", "",
			Plan{InvoiceID: 7, GaveUp: true},
		},
		{
			"delivered invoice added to the settlement",
			mq.SettlementItemAdded, event.ItemAdded{InvoiceID: 7, SettlementID: 5}, domain.InvoiceDelivered, "", "",
			Plan{InvoiceID: 7, InvoiceFrom: domain.InvoiceDelivered, InvoiceTo: domain.InvoiceClosed},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := PlanFor(c.key, mustJSON(t, c.body), c.status, c.kind, orWaiting(c.task))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("PlanFor(%s)\n got %+v\nwant %+v", c.key, got, c.want)
			}
		})
	}
}

func orWaiting(s domain.TaskStatus) domain.TaskStatus {
	if s == "" {
		return domain.TaskWaitingCallback
	}
	return s
}

func TestEvents_SucceededWithoutInvoiceNo_Rejected(t *testing.T) {
	body := mustJSON(t, event.TaskOutcome{TaskID: 11, InvoiceID: 7, Supplier: "air-a"})
	if _, err := PlanFor(mq.ProviderTaskSucceeded, body, domain.InvoiceIssuing, domain.KindIssue, domain.TaskWaitingCallback); err == nil {
		t.Fatal("a success callback without an invoice number was accepted")
	}
}

func TestEvents_UnknownRoutingKey_Rejected(t *testing.T) {
	if _, err := PlanFor("provider.task.exploded", []byte(`{}`), domain.InvoiceIssuing, domain.KindIssue, domain.TaskWaitingCallback); err == nil {
		t.Fatal("unknown routing key was accepted")
	}
}

func TestTaskCreatedEvent_CarriesInvoiceFieldsAndExactAmount(t *testing.T) {
	amt, err := money.Parse("100.10")
	if err != nil {
		t.Fatal(err)
	}
	inv := domain.Invoice{
		ID: 7, ProductLine: "HOTEL", Title: "Example Travel Co., Ltd.", TaxNo: "91310000EXAMPLE001",
		Amount: amt, CustomerID: "CUST-0001",
	}
	env, err := taskCreatedEvent(inv, 11, domain.KindIssue, "trace-1")
	if err != nil {
		t.Fatal(err)
	}
	if env.Type != mq.InvoiceTaskCreated || env.AggregateID != "7" || env.TraceID != "trace-1" {
		t.Fatalf("unexpected envelope: %+v", env)
	}
	var p event.TaskCreated
	if err := env.Decode(&p); err != nil {
		t.Fatal(err)
	}
	if p.TaskID != 11 || p.InvoiceID != 7 || p.CommandKind != "ISSUE" || p.Amount.String() != "100.10" {
		t.Fatalf("unexpected payload: %+v", p)
	}
}
