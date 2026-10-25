package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/mafuky/invoice-hub/internal/platform/config"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestDemo_RoundRobinRetriesAndSummary(t *testing.T) {
	posts := 0
	attempt := 0
	var retried string
	client := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		w := httptest.NewRecorder()
		if r.Method == "POST" {
			var in map[string]string
			b, _ := io.ReadAll(r.Body)
			if err := json.Unmarshal(b, &in); err != nil {
				t.Fatal(err)
			}
			attempt++
			if attempt == 1 {
				retried = in["biz_order_no"]
				w.WriteHeader(503)
				return w.Result(), nil
			}
			if attempt == 2 && retried != in["biz_order_no"] {
				t.Fatal("retry changed business order")
			}
			if !regexp.MustCompile(`^BIZ-\d{8}-\d{4,}$`).MatchString(in["biz_order_no"]) {
				t.Fatal(in)
			}
			if in["product_line"] != []string{"FLIGHT", "TRAIN", "HOTEL"}[posts%3] || in["amount"] != []string{"100.10", "250.00", "1999.99"}[posts%3] {
				t.Fatal(in)
			}
			posts++
			w.WriteHeader(202)
			_ = json.NewEncoder(w).Encode(map[string]any{"invoice_id": posts, "existed": false})
		} else {
			_, _ = w.WriteString(`{"status":"closed"}`)
		}
		return w.Result(), nil
	})}
	var out bytes.Buffer
	err := run(context.Background(), config.Config{APIAddr: ":8080"}, options{N: 3, Timeout: time.Second, Poll: time.Millisecond, Scenario: "scenarios/chaos.yaml"}, client, &out)
	if err != nil {
		t.Fatal(err)
	}
	var s summary
	if err := json.Unmarshal(out.Bytes(), &s); err != nil {
		t.Fatal(err)
	}
	if s.Created != 3 || s.Counts["closed"] != 3 || s.TimedOut || s.StartedAt == "" {
		t.Fatalf("summary=%s", out.String())
	}
	if _, err := time.Parse(time.RFC3339Nano, s.StartedAt); err != nil {
		t.Fatal(err)
	}
}
func TestDemo_TimeoutStillPrintsSummary(t *testing.T) {
	client := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		w := httptest.NewRecorder()
		if r.Method == "POST" {
			w.WriteHeader(202)
			_, _ = w.WriteString(`{"invoice_id":1}`)
		} else {
			_, _ = w.WriteString(`{"status":"issuing"}`)
		}
		return w.Result(), nil
	})}
	var out bytes.Buffer
	if err := run(context.Background(), config.Config{APIAddr: ":8080"}, options{N: 1, Timeout: 10 * time.Millisecond, Poll: time.Millisecond}, client, &out); err == nil {
		t.Fatal("timeout succeeded")
	}
	if !strings.Contains(out.String(), `"timed_out":true`) || !strings.Contains(out.String(), `"issuing":1`) {
		t.Fatal(out.String())
	}
}
