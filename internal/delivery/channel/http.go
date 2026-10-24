// Package channel pushes an issued invoice to the customer's endpoint and to the finance center.
package channel

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

	"github.com/mafuky/invoice-hub/internal/delivery/store"
	"github.com/mafuky/invoice-hub/internal/platform/money"
)

// ErrPush marks a push that was not accepted, including one whose answer never arrived.
// A timed-out push may still have been processed by the receiver.
var ErrPush = errors.New("push failed")

// pushBody carries invoice_id and invoice_version so the receiver can drop a repeated push.
type pushBody struct {
	InvoiceID      string       `json:"invoice_id"`
	InvoiceVersion int          `json:"invoice_version"`
	InvoiceNo      string       `json:"invoice_no"`
	Amount         money.Amount `json:"amount"`
}

// HTTP posts deliveries to one channel of the mock (or the real receiver).
type HTTP struct {
	base string
	path func(d store.Delivery) string
	http *http.Client
}

// NewCustomer pushes to <base>/customers/<customer_id>/push.
func NewCustomer(base string, timeout time.Duration) *HTTP {
	return &HTTP{
		base: strings.TrimRight(base, "/"),
		path: func(d store.Delivery) string { return "/customers/" + url.PathEscape(d.CustomerID) + "/push" },
		http: &http.Client{Timeout: timeout},
	}
}

// NewFinance writes back to <base>/finance/writeback.
func NewFinance(base string, timeout time.Duration) *HTTP {
	return &HTTP{
		base: strings.TrimRight(base, "/"),
		path: func(store.Delivery) string { return "/finance/writeback" },
		http: &http.Client{Timeout: timeout},
	}
}

// Send posts d and succeeds only on a 2xx answer.
func (c *HTTP) Send(ctx context.Context, d store.Delivery) error {
	amt, err := money.Parse(d.Amount)
	if err != nil {
		return fmt.Errorf("push invoice %d v%d to %s: bad amount: %w", d.InvoiceID, d.InvoiceVersion, d.Channel, err)
	}
	raw, err := json.Marshal(pushBody{
		InvoiceID:      strconv.FormatInt(d.InvoiceID, 10),
		InvoiceVersion: d.InvoiceVersion,
		InvoiceNo:      d.InvoiceNo,
		Amount:         amt,
	})
	if err != nil {
		return fmt.Errorf("push invoice %d v%d to %s: encode: %w", d.InvoiceID, d.InvoiceVersion, d.Channel, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+c.path(d), bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("push invoice %d v%d to %s: %w", d.InvoiceID, d.InvoiceVersion, d.Channel, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("push invoice %d v%d to %s: %w: %w", d.InvoiceID, d.InvoiceVersion, d.Channel, ErrPush, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("push invoice %d v%d to %s: %w: status %d", d.InvoiceID, d.InvoiceVersion, d.Channel, ErrPush, resp.StatusCode)
	}
	return nil
}
