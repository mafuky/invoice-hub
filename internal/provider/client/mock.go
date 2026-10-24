// Package client calls the supplier HTTP API; in development that is mock-supplier.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/mafuky/invoice-hub/internal/platform/money"
)

// ErrSupplier marks a supplier that could not be reached or answered with an error.
var ErrSupplier = errors.New("supplier request failed")

// OrderRequest is what a supplier needs to issue one invoice.
type OrderRequest struct {
	TaskID      int64
	InvoiceID   int64
	ProductLine string
	Title       string
	TaxNo       string
	Amount      money.Amount
	Kind        string
}

// QueryResult is the supplier's own view of an order.
type QueryResult struct {
	Found     bool
	Outcome   string
	InvoiceNo string
}

type Mock struct {
	base string
	http *http.Client
}

func NewMock(baseURL string) *Mock {
	return &Mock{base: strings.TrimRight(baseURL, "/"), http: &http.Client{Timeout: 5 * time.Second}}
}

type orderBody struct {
	TaskID      string       `json:"task_id"`
	InvoiceID   string       `json:"invoice_id"`
	ProductLine string       `json:"product_line"`
	Title       string       `json:"title"`
	TaxNo       string       `json:"tax_no"`
	Amount      money.Amount `json:"amount"`
	Kind        string       `json:"kind"`
}

// CreateOrder places an order with the supplier and returns its order number.
// The task id is sent along, so repeating a call for one task returns the same order.
func (m *Mock) CreateOrder(ctx context.Context, supplier string, req OrderRequest) (string, error) {
	raw, err := json.Marshal(orderBody{
		TaskID: strconv.FormatInt(req.TaskID, 10), InvoiceID: strconv.FormatInt(req.InvoiceID, 10),
		ProductLine: req.ProductLine, Title: req.Title, TaxNo: req.TaxNo, Amount: req.Amount, Kind: req.Kind,
	})
	if err != nil {
		return "", fmt.Errorf("encode order for task %d: %w", req.TaskID, err)
	}
	var out struct {
		SupplierOrderNo string `json:"supplier_order_no"`
	}
	if err := m.do(ctx, http.MethodPost, "/suppliers/"+url.PathEscape(supplier)+"/orders", raw, &out); err != nil {
		return "", fmt.Errorf("order at %s for task %d: %w", supplier, req.TaskID, err)
	}
	if out.SupplierOrderNo == "" {
		return "", fmt.Errorf("order at %s for task %d: %w: no order number in reply", supplier, req.TaskID, ErrSupplier)
	}
	return out.SupplierOrderNo, nil
}

// QueryOrder asks the supplier what became of an order.
func (m *Mock) QueryOrder(ctx context.Context, supplier, supplierOrderNo string) (QueryResult, error) {
	var out struct {
		Found     bool   `json:"found"`
		Outcome   string `json:"outcome"`
		InvoiceNo string `json:"invoice_no"`
	}
	path := "/suppliers/" + url.PathEscape(supplier) + "/orders/" + url.PathEscape(supplierOrderNo)
	if err := m.do(ctx, http.MethodGet, path, nil, &out); err != nil {
		return QueryResult{}, fmt.Errorf("query %s/%s: %w", supplier, supplierOrderNo, err)
	}
	return QueryResult{Found: out.Found, Outcome: out.Outcome, InvoiceNo: out.InvoiceNo}, nil
}

func (m *Mock) do(ctx context.Context, method, path string, body []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, m.base+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := m.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: %v", ErrSupplier, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%w: status %d", ErrSupplier, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("%w: bad reply: %v", ErrSupplier, err)
	}
	return nil
}
