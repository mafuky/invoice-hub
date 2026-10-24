package verify

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientLookup_ReadsSupplierRecord(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/verify/HOTEL/HOTEL-A-0001" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"found":true,"amount":"100.10","title":"Example Travel Co., Ltd.","status":"issued"}`))
	}))
	defer srv.Close()

	got, err := NewClient(srv.URL+"/", time.Second).Lookup(context.Background(), "HOTEL", "HOTEL-A-0001")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Found || got.Amount.String() != "100.10" || got.Title != "Example Travel Co., Ltd." || got.Status != "issued" {
		t.Fatalf("unexpected record: %+v", got)
	}
}

func TestClientLookup_UnknownInvoiceIsNotAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"found":false}`))
	}))
	defer srv.Close()

	got, err := NewClient(srv.URL, time.Second).Lookup(context.Background(), "TRAIN", "RAIL-A-0001")
	if err != nil {
		t.Fatal(err)
	}
	if got.Found {
		t.Fatalf("found = true for an unknown invoice: %+v", got)
	}
}

func TestClientLookup_SupplierErrorsAreMarked(t *testing.T) {
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer failing.Close()
	if _, err := NewClient(failing.URL, time.Second).Lookup(context.Background(), "FLIGHT", "AIR-A-0001"); !errors.Is(err, ErrSupplier) {
		t.Fatalf("500 answer: err = %v, want ErrSupplier", err)
	}

	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer slow.Close()
	if _, err := NewClient(slow.URL, 50*time.Millisecond).Lookup(context.Background(), "FLIGHT", "AIR-A-0002"); !errors.Is(err, ErrSupplier) {
		t.Fatalf("timeout: err = %v, want ErrSupplier", err)
	}
}
