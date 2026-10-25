package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mafuky/invoice-hub/internal/platform/config"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/reconcile"
)

type options struct {
	N             int
	Scenario      string
	Timeout, Poll time.Duration
}
type summary struct {
	StartedAt string         `json:"started_at"`
	Scenario  string         `json:"scenario"`
	Requested int            `json:"requested"`
	Created   int            `json:"created"`
	Counts    map[string]int `json:"counts"`
	TimedOut  bool           `json:"timed_out"`
}

func main() {
	var opt options
	flag.IntVar(&opt.N, "n", 100, "number of invoices")
	flag.StringVar(&opt.Scenario, "scenario", "scenarios/chaos.yaml", "informational scenario; start the mock separately")
	flag.DurationVar(&opt.Timeout, "timeout", 10*time.Minute, "total run timeout")
	flag.DurationVar(&opt.Poll, "poll", time.Second, "poll and retry interval")
	flag.Parse()
	if opt.N <= 0 || opt.Timeout <= 0 || opt.Poll <= 0 {
		fmt.Fprintln(os.Stderr, "demo: n, timeout and poll must be positive")
		os.Exit(2)
	}
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "demo: configuration could not be loaded; check INVHUB settings")
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, opt, &http.Client{Timeout: 15 * time.Second}, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func apiURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err == nil && (host == "" || host == "0.0.0.0" || host == "::") {
		addr = net.JoinHostPort("127.0.0.1", port)
	}
	return "http://" + addr
}
func pause(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func exchange(ctx context.Context, client *http.Client, method, url string, body []byte, out any, interval time.Duration) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
		if err != nil {
			return errors.New("demo: invalid API address")
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err == nil {
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				err = json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(out)
				_ = resp.Body.Close()
				if err == nil {
					return nil
				}
				// An interrupted response may follow a committed Apply; retry the same order.
			} else {
				code := resp.StatusCode
				_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
				_ = resp.Body.Close()
				if code != 408 && code != 429 && (code < 500 || code == 501) {
					return fmt.Errorf("demo: API returned HTTP %d", code)
				}
			}
		}
		if err := pause(ctx, interval); err != nil {
			return err
		}
	}
}
func run(parent context.Context, cfg config.Config, opt options, client *http.Client, w io.Writer) (runErr error) {
	if opt.N <= 0 || opt.Timeout <= 0 || opt.Poll <= 0 {
		return errors.New("demo: n, timeout and poll must be positive")
	}
	// Millisecond precision includes invoices stored in DATETIME(3) at the run boundary.
	start := time.Now().UTC().Truncate(time.Millisecond)
	s := summary{StartedAt: start.Format(time.RFC3339Nano), Scenario: opt.Scenario, Requested: opt.N, Counts: map[string]int{}}
	states := map[int64]string{}
	ctx, cancel := context.WithTimeout(parent, opt.Timeout)
	defer cancel()
	defer func() {
		s.TimedOut = errors.Is(ctx.Err(), context.DeadlineExceeded)
		for _, state := range states {
			s.Counts[state]++
		}
		if err := json.NewEncoder(w).Encode(s); err != nil && runErr == nil {
			runErr = errors.New("demo: could not write summary")
		}
	}()
	// A random decimal prefix keeps separate runs' business orders distinct.
	seed, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 80))
	if err != nil {
		return errors.New("demo: could not generate order sequence")
	}
	base := strings.TrimRight(apiURL(cfg.APIAddr), "/")
	for i := 0; i < opt.N; i++ {
		amt, _ := money.Parse([]string{"100.10", "250.00", "1999.99"}[i%3])
		in := struct {
			BizOrderNo  string       `json:"biz_order_no"`
			ProductLine string       `json:"product_line"`
			Title       string       `json:"title"`
			TaxNo       string       `json:"tax_no"`
			Amount      money.Amount `json:"amount"`
			CustomerID  string       `json:"customer_id"`
		}{fmt.Sprintf("BIZ-%s-%s%04d", start.Format("20060102"), seed.String(), i+1), []string{"FLIGHT", "TRAIN", "HOTEL"}[i%3], "Example Demo Company (FAKE)", "FAKE-TAX-DEMO-0001", amt, "demo-customer"}
		body, _ := json.Marshal(in)
		var reply struct {
			InvoiceID int64 `json:"invoice_id"`
			Existed   bool  `json:"existed"`
		}
		if err := exchange(ctx, client, "POST", base+"/invoices", body, &reply, opt.Poll); err != nil {
			return err
		}
		if reply.InvoiceID <= 0 {
			return errors.New("demo: API returned an invalid invoice id")
		}
		if _, exists := states[reply.InvoiceID]; exists {
			return errors.New("demo: distinct business orders returned the same invoice")
		}
		states[reply.InvoiceID] = "unknown"
		s.Created++
	}
	for {
		pending := false
		for id, state := range states {
			if reconcile.Terminal(state) {
				continue
			}
			var reply struct {
				Status string `json:"status"`
			}
			if err := exchange(ctx, client, "GET", fmt.Sprintf("%s/invoices/%d", base, id), nil, &reply, opt.Poll); err != nil {
				return err
			}
			if reply.Status == "" {
				return errors.New("demo: API returned an empty invoice status")
			}
			states[id] = reply.Status
			if !reconcile.Terminal(reply.Status) {
				pending = true
			}
		}
		if !pending {
			return nil
		}
		if err := pause(ctx, opt.Poll); err != nil {
			return err
		}
	}
}
