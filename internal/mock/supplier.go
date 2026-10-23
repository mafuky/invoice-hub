package mock

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/mafuky/invoice-hub/internal/platform/money"
)

const (
	outcomePending   = "PENDING"
	outcomeSucceeded = "SUCCEEDED"
	outcomeFailed    = "FAILED"
)

var validKinds = map[string]bool{"ISSUE": true, "RED_FLUSH": true, "REISSUE": true, "SUPPLEMENT": true}

type orderReq struct {
	TaskID      string        `json:"task_id"`
	InvoiceID   string        `json:"invoice_id"`
	ProductLine string        `json:"product_line"`
	Title       string        `json:"title"`
	TaxNo       string        `json:"tax_no"`
	Amount      *money.Amount `json:"amount"`
	Kind        string        `json:"kind"`
}

type callbackBody struct {
	Supplier        string `json:"supplier"`
	SupplierOrderNo string `json:"supplier_order_no"`
	EventSeq        int    `json:"event_seq"`
	Outcome         string `json:"outcome"`
	InvoiceNo       string `json:"invoice_no"`
	Kind            string `json:"kind"`
}

// step is one planned callback delivery.
type step struct {
	delay time.Duration
	body  callbackBody
}

type order struct {
	no      string
	req     orderReq
	hidden  bool // supplier never reports back, so queries find nothing
	outcome string
	invNo   string
}

type supplier struct {
	cfg    SupplierCfg
	rng    *rng
	ctx    context.Context
	wg     *sync.WaitGroup
	cbBase string
	client *http.Client
	verify *verifyData
	log    *slog.Logger
	lines  map[string]bool

	mu     sync.Mutex
	seq    int
	orders map[string]*order
	byTask map[string]string
}

func newSupplier(ctx context.Context, wg *sync.WaitGroup, cfg SupplierCfg, seed uint64, cbBase string, v *verifyData, log *slog.Logger) *supplier {
	lines := map[string]bool{}
	for _, pl := range cfg.ProductLines {
		lines[pl] = true
	}
	var h uint64 = 14695981039346656037
	for _, c := range []byte(cfg.Name) {
		h = (h ^ uint64(c)) * 1099511628211
	}
	return &supplier{
		cfg: cfg, rng: newRNG(seed, h), ctx: ctx, wg: wg,
		cbBase: strings.TrimRight(cbBase, "/"), client: &http.Client{Timeout: 5 * time.Second},
		verify: v, log: log, lines: lines,
		orders: map[string]*order{}, byTask: map[string]string{},
	}
}

func (s *supplier) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-s.ctx.Done():
		return false
	}
}

func (s *supplier) createOrder(w http.ResponseWriter, r *http.Request) {
	var req orderReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid order body")
		return
	}
	if req.Kind == "" {
		req.Kind = "ISSUE"
	}
	switch {
	case !validKinds[req.Kind]:
		writeErr(w, http.StatusBadRequest, "unknown kind")
		return
	case req.Amount == nil:
		writeErr(w, http.StatusBadRequest, "amount is required")
		return
	case !s.lines[req.ProductLine]:
		writeErr(w, http.StatusBadRequest, "product_line not served by this supplier")
		return
	}

	if !s.sleep(s.rng.between(s.cfg.OrderLatencyMS)) {
		writeErr(w, http.StatusServiceUnavailable, "shutting down")
		return
	}
	if s.rng.hit(s.cfg.OrderFailRate) {
		writeErr(w, http.StatusServiceUnavailable, "injected failure")
		return
	}

	s.mu.Lock()
	// A repeated task id gets the original order back instead of a second one.
	if req.TaskID != "" {
		if no, ok := s.byTask[req.TaskID]; ok {
			s.mu.Unlock()
			writeJSON(w, http.StatusOK, map[string]string{"supplier_order_no": no})
			return
		}
	}
	s.seq++
	o := &order{no: fmt.Sprintf("%s-ORD-%06d", strings.ToUpper(s.cfg.Name), s.seq), req: req, outcome: outcomePending}
	s.orders[o.no] = o
	if req.TaskID != "" {
		s.byTask[req.TaskID] = o.no
	}
	s.mu.Unlock()

	steps := s.plan(o)
	if steps == nil {
		s.mu.Lock()
		o.hidden = true
		s.mu.Unlock()
	} else {
		s.wg.Add(1)
		go s.deliver(o, steps)
	}
	writeJSON(w, http.StatusOK, map[string]string{"supplier_order_no": o.no})
}

// plan draws every random decision for an order up front. A nil result
// means the supplier will never call back.
func (s *supplier) plan(o *order) []step {
	if s.rng.hit(s.cfg.NoCallbackRate) {
		return nil
	}
	first := s.rng.between(s.cfg.CallbackDelayMS)
	failed := s.rng.hit(s.cfg.CallbackFailRate)
	ooo := !failed && s.rng.hit(s.cfg.OutOfOrderRate)
	dups := 0
	if s.rng.hit(s.cfg.DuplicateRate) {
		dups = 1 + s.rng.intn(3)
	}

	mk := func(seq int, outcome, inv string) callbackBody {
		return callbackBody{Supplier: s.cfg.Name, SupplierOrderNo: o.no, EventSeq: seq,
			Outcome: outcome, InvoiceNo: inv, Kind: o.req.Kind}
	}
	var steps []step
	switch {
	case failed:
		steps = []step{{first, mk(1, outcomeFailed, "")}}
	case ooo:
		inv := s.newInvoiceNo()
		steps = []step{
			{first, mk(1, outcomeFailed, "")},
			{s.rng.between(s.cfg.CallbackDelayMS), mk(2, outcomeSucceeded, inv)},
		}
	default:
		steps = []step{{first, mk(1, outcomeSucceeded, s.newInvoiceNo())}}
	}
	last := steps[len(steps)-1]
	for range dups {
		steps = append(steps, step{s.rng.between(s.cfg.CallbackDelayMS), last.body})
	}
	return steps
}

func (s *supplier) newInvoiceNo() string {
	return fmt.Sprintf("%s-%08d", strings.ToUpper(s.cfg.Name), s.rng.intn(100_000_000))
}

func (s *supplier) deliver(o *order, steps []step) {
	defer s.wg.Done()
	for _, st := range steps {
		if !s.sleep(st.delay) {
			return
		}
		s.decide(o, st.body)
		s.post(st.body)
	}
}

// decide records the outcome the supplier has just reached.
func (s *supplier) decide(o *order, b callbackBody) {
	if b.Outcome == outcomeSucceeded {
		s.verify.register(b.InvoiceNo, o.req.ProductLine, *o.req.Amount, o.req.Title, o.req.Kind)
	}
	s.mu.Lock()
	o.outcome, o.invNo = b.Outcome, b.InvoiceNo
	s.mu.Unlock()
}

func (s *supplier) post(b callbackBody) {
	raw, _ := json.Marshal(b)
	req, err := http.NewRequestWithContext(s.ctx, http.MethodPost, s.cbBase+"/"+s.cfg.Name, bytes.NewReader(raw))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		s.log.Warn("callback delivery failed", "supplier", s.cfg.Name, "order", b.SupplierOrderNo, "err", err)
		return
	}
	resp.Body.Close()
}

func (s *supplier) queryOrder(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	o, ok := s.orders[r.PathValue("supplier_order_no")]
	var out map[string]any
	if ok && !o.hidden {
		out = map[string]any{"found": true, "outcome": o.outcome, "invoice_no": o.invNo}
	}
	s.mu.Unlock()
	if out == nil {
		out = map[string]any{"found": false, "outcome": "", "invoice_no": ""}
	}
	writeJSON(w, http.StatusOK, out)
}
