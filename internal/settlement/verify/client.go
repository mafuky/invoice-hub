package verify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/mafuky/invoice-hub/internal/platform/money"
)

const defaultTimeout = 2 * time.Second

// ErrSupplier marks a lookup that got no usable answer from the supplier side.
var ErrSupplier = errors.New("supplier verify unavailable")

// Client asks the supplier side (the mock, in this repo) for its record of an invoice.
type Client struct {
	base string
	http *http.Client
}

// NewClient returns a client for <base>/verify/...; a timeout of zero or less means two seconds.
func NewClient(base string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	return &Client{base: strings.TrimRight(base, "/"), http: &http.Client{Timeout: timeout}}
}

type verifyReply struct {
	Found  bool         `json:"found"`
	Amount money.Amount `json:"amount"`
	Title  string       `json:"title"`
	Status string       `json:"status"`
}

// Lookup fetches the supplier's record of invoiceNo for the product line.
// An invoice the supplier does not know is not an error: Found is false.
func (c *Client) Lookup(ctx context.Context, line, invoiceNo string) (SupplierInvoice, error) {
	u := c.base + "/verify/" + url.PathEscape(line) + "/" + url.PathEscape(invoiceNo)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return SupplierInvoice{}, fmt.Errorf("verify: build request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return SupplierInvoice{}, fmt.Errorf("%w: invoice %s: %w", ErrSupplier, invoiceNo, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return SupplierInvoice{}, fmt.Errorf("%w: invoice %s: status %d", ErrSupplier, invoiceNo, resp.StatusCode)
	}
	var r verifyReply
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return SupplierInvoice{}, fmt.Errorf("%w: invoice %s: bad answer: %w", ErrSupplier, invoiceNo, err)
	}
	return SupplierInvoice{Found: r.Found, Amount: r.Amount, Title: r.Title, Status: r.Status}, nil
}
