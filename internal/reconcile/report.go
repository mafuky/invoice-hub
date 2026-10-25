// Package reconcile checks invoice, delivery and settlement records without I/O.
package reconcile

import (
	"fmt"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"time"
)

type Invoice struct {
	ID         int64
	CustomerID string
	Status     string
	Amount     money.Amount
	// Versions are the versions in invoice.issued events, not the latest state version.
	Versions []int
}
type Delivery struct {
	InvoiceID int64
	Version   int
	Channel   string
	Status    string
}
type Receipt struct {
	InvoiceID int64
	Version   int
	Channel   string
}
type Item struct {
	InvoiceID int64
	Amount    money.Amount
}
type Outbox struct {
	Status    string
	CreatedAt time.Time
}
type Snapshot struct {
	Invoices   []Invoice
	Deliveries []Delivery
	Receipts   []Receipt
	Items      []Item
	Outbox     []Outbox
}
type Report struct{ Total, Closed, Stuck, DupDelivery, DupSettlement, AmountMismatch, OutboxBacklog int }

func (r Report) String() string {
	return fmt.Sprintf("RECONCILE total=%d closed=%d stuck=%d dup_delivery=%d dup_settlement=%d amount_mismatch=%d outbox_backlog=%d", r.Total, r.Closed, r.Stuck, r.DupDelivery, r.DupSettlement, r.AmountMismatch, r.OutboxBacklog)
}
func (r Report) HasProblems() bool {
	return r.Stuck > 0 || r.DupDelivery > 0 || r.DupSettlement > 0 || r.AmountMismatch > 0 || r.OutboxBacklog > 0
}
func (r Report) ExitCode() int {
	if r.HasProblems() {
		return 1
	}
	return 0
}
func Terminal(status string) bool {
	return status == "closed" || status == "failed" || status == "red_flushed"
}

// Count counts duplicate channel/version groups and affected settlement invoices.
// Missing records or unfinished delivery count an invoice once in stuck.
func Count(s Snapshot, now time.Time) Report {
	r := Report{Total: len(s.Invoices)}
	deliveries := map[int64][]Delivery{}
	receipts := map[int64][]Receipt{}
	items := map[int64][]Item{}
	for _, d := range s.Deliveries {
		deliveries[d.InvoiceID] = append(deliveries[d.InvoiceID], d)
	}
	for _, p := range s.Receipts {
		receipts[p.InvoiceID] = append(receipts[p.InvoiceID], p)
	}
	for _, it := range s.Items {
		items[it.InvoiceID] = append(items[it.InvoiceID], it)
	}
	type key struct {
		version int
		channel string
	}
	for _, in := range s.Invoices {
		if in.Status == "closed" {
			r.Closed++
		}
		stuck := !Terminal(in.Status)
		versions := map[int]bool{}
		dc := map[key]int{}
		rc := map[key]int{}
		for _, v := range in.Versions {
			versions[v] = true
		}
		for _, d := range deliveries[in.ID] {
			versions[d.Version] = true
			dc[key{d.Version, d.Channel}]++
			if d.Status != "delivered" {
				stuck = true
			}
		}
		for _, p := range receipts[in.ID] {
			versions[p.Version] = true
			rc[key{p.Version, p.Channel}]++
		}
		if len(versions) == 0 {
			stuck = true
		}
		for v := range versions {
			for _, channel := range []string{"customer", "finance"} {
				k := key{v, channel}
				if dc[k] == 0 || rc[k] == 0 {
					stuck = true
				}
				if dc[k] > 1 || rc[k] > 1 {
					r.DupDelivery++
				}
			}
		}
		if len(items[in.ID]) == 0 {
			stuck = true
		}
		if len(items[in.ID]) > 1 {
			r.DupSettlement++
		}
		for _, it := range items[in.ID] {
			if !it.Amount.Equal(in.Amount) {
				r.AmountMismatch++
				break
			}
		}
		if stuck {
			r.Stuck++
		}
	}
	for _, o := range s.Outbox {
		if (o.Status == "new" || o.Status == "pending") && o.CreatedAt.Before(now.Add(-time.Minute)) {
			r.OutboxBacklog++
		}
	}
	return r
}
