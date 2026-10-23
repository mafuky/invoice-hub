package event

import (
	"strings"
	"testing"

	"github.com/mafuky/invoice-hub/internal/platform/money"
)

func TestTaskCreated_AmountTravelsAsJSONString(t *testing.T) {
	amt, err := money.Parse("1999.99")
	if err != nil {
		t.Fatal(err)
	}
	in := TaskCreated{
		TaskID: 7, InvoiceID: 42, CommandKind: "ISSUE", ProductLine: "FLIGHT",
		Title: "Example Travel Co", TaxNo: "91310000XXXXXXXXXX", Amount: amt, CustomerID: "cust-001",
	}
	e, err := New("invoice.task.created", "42", 1, in, "tr-1")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(e.Payload), `"amount":"1999.99"`) {
		t.Fatalf("amount is not a JSON string: %s", e.Payload)
	}
	var out TaskCreated
	if err := e.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.TaskID != 7 || out.InvoiceID != 42 || out.CustomerID != "cust-001" || !out.Amount.Equal(amt) {
		t.Fatalf("round trip lost data: %+v", out)
	}
}
