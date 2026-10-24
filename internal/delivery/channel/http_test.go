package channel

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/mafuky/invoice-hub/internal/delivery/store"
)

type seen struct {
	path string
	body map[string]any
}

func recorder(t *testing.T, status int, hold time.Duration) (*httptest.Server, func() []seen) {
	t.Helper()
	var mu sync.Mutex
	var got []seen
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		mu.Lock()
		got = append(got, seen{path: r.URL.Path, body: b})
		mu.Unlock()
		if hold > 0 {
			select {
			case <-time.After(hold):
			case <-r.Context().Done():
				return
			}
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []seen {
		mu.Lock()
		defer mu.Unlock()
		return append([]seen{}, got...)
	}
}

func delivery(channel string) store.Delivery {
	return store.Delivery{
		InvoiceID: 3001, InvoiceVersion: 2, Channel: channel, CustomerID: "CUST-0001",
		InvoiceNo: "AIR-A-12345678", Amount: "1999.99",
	}
}

func TestCustomerSend_PostsInvoiceIdentityToCustomerEndpoint(t *testing.T) {
	srv, got := recorder(t, http.StatusOK, 0)
	if err := NewCustomer(srv.URL+"/", time.Second).Send(context.Background(), delivery(store.ChannelCustomer)); err != nil {
		t.Fatal(err)
	}
	reqs := got()
	if len(reqs) != 1 || reqs[0].path != "/customers/CUST-0001/push" {
		t.Fatalf("requests = %+v", reqs)
	}
	b := reqs[0].body
	if b["invoice_id"] != "3001" || b["invoice_version"] != float64(2) || b["invoice_no"] != "AIR-A-12345678" || b["amount"] != "1999.99" {
		t.Fatalf("body = %v", b)
	}
}

func TestFinanceSend_PostsToWriteback(t *testing.T) {
	srv, got := recorder(t, http.StatusOK, 0)
	if err := NewFinance(srv.URL, time.Second).Send(context.Background(), delivery(store.ChannelFinance)); err != nil {
		t.Fatal(err)
	}
	if reqs := got(); len(reqs) != 1 || reqs[0].path != "/finance/writeback" {
		t.Fatalf("requests = %+v", reqs)
	}
}

func TestSend_ServerErrorIsPushFailure(t *testing.T) {
	srv, _ := recorder(t, http.StatusInternalServerError, 0)
	err := NewCustomer(srv.URL, time.Second).Send(context.Background(), delivery(store.ChannelCustomer))
	if !errors.Is(err, ErrPush) {
		t.Fatalf("err = %v, want ErrPush", err)
	}
}

func TestSend_SlowAnswerTimesOutAfterReceiverRecordedIt(t *testing.T) {
	srv, got := recorder(t, http.StatusOK, 2*time.Second)
	err := NewCustomer(srv.URL, 100*time.Millisecond).Send(context.Background(), delivery(store.ChannelCustomer))
	if !errors.Is(err, ErrPush) {
		t.Fatalf("err = %v, want ErrPush", err)
	}
	if n := len(got()); n != 1 {
		t.Fatalf("receiver saw %d pushes, want 1", n)
	}
}

func TestSend_RejectsUnparsableAmountWithoutCalling(t *testing.T) {
	srv, got := recorder(t, http.StatusOK, 0)
	d := delivery(store.ChannelCustomer)
	d.Amount = "n/a"
	if err := NewCustomer(srv.URL, time.Second).Send(context.Background(), d); err == nil {
		t.Fatal("expected an error")
	}
	if n := len(got()); n != 0 {
		t.Fatalf("receiver saw %d pushes, want 0", n)
	}
}
