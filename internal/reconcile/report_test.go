package reconcile

import (
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"testing"
	"time"
)

func amount(s string) money.Amount {
	a, err := money.Parse(s)
	if err != nil {
		panic(err)
	}
	return a
}
func TestReport_CountsAndExitCode(t *testing.T) {
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	s := Snapshot{Invoices: []Invoice{{ID: 1, Status: "closed", Amount: amount("100.10"), Versions: []int{2}}},
		Deliveries: []Delivery{{1, 2, "customer", "delivered"}, {1, 2, "customer", "delivered"}, {1, 2, "finance", "delivered"}},
		Receipts:   []Receipt{{1, 2, "customer"}, {1, 2, "finance"}}, Items: []Item{{1, amount("100.11")}}}
	r := Count(s, now)
	want := "RECONCILE total=1 closed=1 stuck=0 dup_delivery=1 dup_settlement=0 amount_mismatch=1 outbox_backlog=0"
	if r.String() != want || !r.HasProblems() || r.ExitCode() != 1 {
		t.Fatalf("report=%s exit=%d", r, r.ExitCode())
	}
	s.Deliveries = s.Deliveries[1:]
	s.Items[0].Amount = amount("100.10")
	if r = Count(s, now); r.HasProblems() || r.ExitCode() != 0 {
		t.Fatalf("clean report=%s", r)
	}
}
func TestReport_MissingRecordsVersionsAndBacklog(t *testing.T) {
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	s := Snapshot{Invoices: []Invoice{{ID: 1, Status: "closed", Amount: amount("100.10"), Versions: []int{2, 5}}, {ID: 2, Status: "issuing", Amount: amount("250.00")}},
		Deliveries: []Delivery{{1, 2, "customer", "delivered"}, {1, 2, "finance", "delivered"}}, Receipts: []Receipt{{1, 2, "customer"}, {1, 2, "customer"}, {1, 2, "finance"}},
		Items:  []Item{{1, amount("100.10")}, {1, amount("100.10")}},
		Outbox: []Outbox{{"new", now.Add(-2 * time.Minute)}, {"pending", now.Add(-2 * time.Minute)}, {"new", now.Add(-time.Minute)}, {"sent", now.Add(-time.Hour)}}}
	r := Count(s, now)
	if r.String() != "RECONCILE total=2 closed=1 stuck=2 dup_delivery=0 dup_settlement=1 amount_mismatch=0 outbox_backlog=2" {
		t.Fatal(r.String())
	}
}

func TestReport_DeliveryDocumentsAndReceipts(t *testing.T) {
	for _, tc := range []struct {
		name       string
		deliveries []Delivery
		receipts   []Receipt
		want       Report
		wantExit   int
	}{
		{
			name:       "repeated receipt confirms arrival",
			deliveries: []Delivery{{1, 2, "customer", "delivered"}, {1, 2, "finance", "delivered"}},
			receipts:   []Receipt{{1, 2, "customer"}, {1, 2, "customer"}, {1, 2, "finance"}},
			want:       Report{Total: 1, Closed: 1},
		},
		{
			name:       "duplicate customer documents",
			deliveries: []Delivery{{1, 2, "customer", "delivered"}, {1, 2, "customer", "delivered"}, {1, 2, "finance", "delivered"}},
			receipts:   []Receipt{{1, 2, "customer"}, {1, 2, "finance"}},
			want:       Report{Total: 1, Closed: 1, DupDelivery: 1}, wantExit: 1,
		},
		{
			name:       "both channels duplicated count one version",
			deliveries: []Delivery{{1, 2, "customer", "delivered"}, {1, 2, "customer", "delivered"}, {1, 2, "finance", "delivered"}, {1, 2, "finance", "delivered"}},
			receipts:   []Receipt{{1, 2, "customer"}, {1, 2, "finance"}},
			want:       Report{Total: 1, Closed: 1, DupDelivery: 1}, wantExit: 1,
		},
		{
			name:       "delivered customer document without receipt",
			deliveries: []Delivery{{1, 2, "customer", "delivered"}, {1, 2, "finance", "delivered"}},
			receipts:   []Receipt{{1, 2, "finance"}},
			want:       Report{Total: 1, Closed: 1, Stuck: 1}, wantExit: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := Snapshot{
				Invoices:   []Invoice{{ID: 1, Status: "closed", Amount: amount("100.10"), Versions: []int{2}}},
				Deliveries: tc.deliveries, Receipts: tc.receipts,
				Items: []Item{{1, amount("100.10")}},
			}
			got := Count(s, time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC))
			if got != tc.want || got.ExitCode() != tc.wantExit {
				t.Fatalf("report=%s exit=%d; want %s exit=%d", got, got.ExitCode(), tc.want, tc.wantExit)
			}
		})
	}
}
