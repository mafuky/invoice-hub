package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mafuky/invoice-hub/internal/reconcile"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestReceipts_FilterInvoicesAndKeepDuplicates(t *testing.T) {
	calls := map[string]int{}
	client := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		calls[r.URL.EscapedPath()]++
		w := httptest.NewRecorder()
		_, _ = w.WriteString(`{"count":3,"pushes":[{"invoice_id":"41","invoice_version":2},{"invoice_id":"41","invoice_version":2},{"invoice_id":"9","invoice_version":2}]}`)
		return w.Result(), nil
	})}
	got, err := loadReceipts(context.Background(), client, "http://mock.test", []reconcile.Invoice{{ID: 41, CustomerID: "demo customer"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 || calls["/customers/demo%20customer/received"] != 1 || calls["/finance/received"] != 1 {
		t.Fatalf("receipts=%v calls=%v", got, calls)
	}
	for _, p := range got {
		if p.InvoiceID != 41 || p.Version != 2 {
			t.Fatal(p)
		}
	}
}
func TestReceipts_ReadFailureIsSanitized(t *testing.T) {
	client := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		w := httptest.NewRecorder()
		w.WriteHeader(503)
		_, _ = w.WriteString("secret-password")
		return w.Result(), nil
	})}
	_, err := loadReceipts(context.Background(), client, "http://user:secret-password@mock.test", nil)
	if err == nil || strings.Contains(err.Error(), "secret-password") {
		t.Fatalf("err=%v", err)
	}
}
