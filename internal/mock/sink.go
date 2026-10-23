package mock

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/mafuky/invoice-hub/internal/platform/money"
)

type pushBody struct {
	InvoiceID      string        `json:"invoice_id"`
	InvoiceVersion int           `json:"invoice_version"`
	InvoiceNo      string        `json:"invoice_no"`
	Amount         *money.Amount `json:"amount"`
}

type pushRecord struct {
	InvoiceID      string `json:"invoice_id"`
	InvoiceVersion int    `json:"invoice_version"`
	InvoiceNo      string `json:"invoice_no"`
	At             string `json:"at"`
}

// sink accepts pushes (customer endpoint or finance center) and remembers them.
type sink struct {
	cfg SinkCfg
	rng *rng
	ctx context.Context

	mu       sync.Mutex
	received map[string][]pushRecord
}

func newSink(ctx context.Context, cfg SinkCfg, seed, stream uint64) *sink {
	if cfg.SlowDelayMS == 0 {
		cfg.SlowDelayMS = defaultSlowDelayMS
	}
	return &sink{cfg: cfg, rng: newRNG(seed, stream), ctx: ctx, received: map[string][]pushRecord{}}
}

// push handles one request; key groups records (customer id or "finance").
func (s *sink) push(w http.ResponseWriter, r *http.Request, key string) {
	var b pushBody
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil || b.Amount == nil || b.InvoiceID == "" {
		writeErr(w, http.StatusBadRequest, "invalid push body")
		return
	}
	if s.rng.hit(s.cfg.FailRate) {
		writeErr(w, http.StatusInternalServerError, "injected failure")
		return
	}
	slow := s.rng.hit(s.cfg.SlowSuccessRate)

	s.mu.Lock()
	s.received[key] = append(s.received[key], pushRecord{
		InvoiceID:      b.InvoiceID,
		InvoiceVersion: b.InvoiceVersion,
		InvoiceNo:      b.InvoiceNo,
		At:             time.Now().UTC().Format(time.RFC3339Nano),
	})
	s.mu.Unlock()

	if slow {
		// The work is done; hold the response past the caller's timeout.
		select {
		case <-time.After(time.Duration(s.cfg.SlowDelayMS) * time.Millisecond):
		case <-r.Context().Done():
			return
		case <-s.ctx.Done():
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *sink) list(w http.ResponseWriter, key string) {
	s.mu.Lock()
	pushes := append([]pushRecord{}, s.received[key]...)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"count": len(pushes), "pushes": pushes})
}
