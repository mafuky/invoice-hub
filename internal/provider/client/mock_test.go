package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mafuky/invoice-hub/internal/platform/money"
)

func TestCreateOrder_SendsKindAndExactAmount(t *testing.T) {
	var path string
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&body)
		_, _ = w.Write([]byte(`{"supplier_order_no":"AIR-A-ORD-000001"}`))
	}))
	t.Cleanup(srv.Close)

	amt, err := money.Parse("1999.90")
	if err != nil {
		t.Fatal(err)
	}
	no, err := NewMock(srv.URL).CreateOrder(context.Background(), "air-a", OrderRequest{
		TaskID: 11, InvoiceID: 7, ProductLine: "FLIGHT", Title: "Example Travel Co., Ltd.",
		TaxNo: "91310000EXAMPLE001", Amount: amt, Kind: "REISSUE",
	})
	if err != nil || no != "AIR-A-ORD-000001" {
		t.Fatalf("order no %q err %v", no, err)
	}
	if path != "/suppliers/air-a/orders" {
		t.Fatalf("path = %q", path)
	}
	if body["kind"] != "REISSUE" || body["amount"] != "1999.90" || body["task_id"] != "11" {
		t.Fatalf("body = %v, want kind REISSUE, amount \"1999.90\", task_id \"11\"", body)
	}
}

func TestCreateOrder_SupplierRefuses_ErrSupplier(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"injected failure"}`, http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	_, err := NewMock(srv.URL).CreateOrder(context.Background(), "air-a", OrderRequest{TaskID: 1})
	if !errors.Is(err, ErrSupplier) {
		t.Fatalf("err = %v, want ErrSupplier", err)
	}
}
