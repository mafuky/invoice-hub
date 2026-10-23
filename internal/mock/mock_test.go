package mock

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLoadScenario_RejectsInvalid(t *testing.T) {
	base := `
suppliers:
  - name: air-a
    product_lines: [FLIGHT]
    order_latency_ms: [1, 5]
    callback_delay_ms: [1, 5]
%s
seed: 1
`
	cases := []struct {
		name, yaml, want string
	}{
		{"rate above 1", strings.Replace(base, "%s", "    duplicate_rate: 1.2", 1), "duplicate_rate"},
		{"negative rate", strings.Replace(base, "%s", "    order_fail_rate: -0.1", 1), "order_fail_rate"},
		{"inverted delay", strings.Replace(strings.Replace(base, "callback_delay_ms: [1, 5]", "callback_delay_ms: [300, 50]", 1), "%s", "", 1), "callback_delay_ms"},
		{"unknown product line", strings.Replace(strings.Replace(base, "[FLIGHT]", "[BUS]", 1), "%s", "", 1), "product_lines"},
		{"no suppliers", "suppliers: []\nseed: 1\n", "suppliers"},
		{"sink rate", strings.Replace(base, "%s", "", 1) + "customer_push: {fail_rate: 2}\n", "customer_push.fail_rate"},
		{"verify rate", strings.Replace(base, "%s", "", 1) + "verify: {missing_rate: -1}\n", "verify.missing_rate"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "s.yaml")
			if err := os.WriteFile(p, []byte(c.yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadScenario(p)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %q does not name %q", err, c.want)
			}
		})
	}
}

func TestLoadScenario_ShippedFiles(t *testing.T) {
	for _, f := range []string{"happy.yaml", "chaos.yaml"} {
		sc, err := LoadScenario(filepath.Join("..", "..", "scenarios", f))
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		if len(sc.Suppliers) != 4 {
			t.Fatalf("%s: want 4 suppliers, got %d", f, len(sc.Suppliers))
		}
	}
}

type cb struct {
	Supplier        string `json:"supplier"`
	SupplierOrderNo string `json:"supplier_order_no"`
	EventSeq        int    `json:"event_seq"`
	Outcome         string `json:"outcome"`
	InvoiceNo       string `json:"invoice_no"`
	Kind            string `json:"kind"`
}

type collector struct {
	mu   sync.Mutex
	got  []cb
	srv  *httptest.Server
	path []string
}

func newCollector(t *testing.T) *collector {
	t.Helper()
	c := &collector{}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b cb
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		c.mu.Lock()
		c.got = append(c.got, b)
		c.path = append(c.path, r.URL.Path)
		c.mu.Unlock()
	}))
	t.Cleanup(c.srv.Close)
	return c
}

func (c *collector) snapshot() []cb {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]cb(nil), c.got...)
}

func (c *collector) waitFor(t *testing.T, n int) []cb {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if g := c.snapshot(); len(g) >= n {
			return g
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d callbacks, got %d", n, len(c.snapshot()))
	return nil
}

func tinySupplier(mod func(*SupplierCfg)) Scenario {
	s := SupplierCfg{
		Name:            "air-a",
		ProductLines:    []string{"FLIGHT"},
		OrderLatencyMS:  [2]int{1, 5},
		CallbackDelayMS: [2]int{1, 5},
	}
	if mod != nil {
		mod(&s)
	}
	return Scenario{Suppliers: []SupplierCfg{s}, Seed: 7}
}

func newMock(t *testing.T, sc Scenario, callbackBase string) *httptest.Server {
	t.Helper()
	m := NewServer(sc, callbackBase)
	ts := httptest.NewServer(m)
	t.Cleanup(func() { ts.Close(); m.Close() })
	return ts
}

func postJSON(t *testing.T, url string, body any) (*http.Response, []byte) {
	t.Helper()
	raw, _ := json.Marshal(body)
	resp, err := http.Post(url, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	return resp, buf.Bytes()
}

func getJSON(t *testing.T, url string, out any) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatal(err)
		}
	}
	return resp.StatusCode
}

func placeOrder(t *testing.T, ts *httptest.Server, extra map[string]any) string {
	t.Helper()
	body := map[string]any{
		"task_id": "task-1", "invoice_id": "inv-1", "product_line": "FLIGHT",
		"title": "ACME Ltd", "tax_no": "91330000X", "amount": "100.10",
	}
	for k, v := range extra {
		body[k] = v
	}
	resp, raw := postJSON(t, ts.URL+"/suppliers/air-a/orders", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("order status %d: %s", resp.StatusCode, raw)
	}
	var out struct {
		No string `json:"supplier_order_no"`
	}
	if err := json.Unmarshal(raw, &out); err != nil || out.No == "" {
		t.Fatalf("bad order response %s", raw)
	}
	return out.No
}

func TestSupplier_HappyPathCallbackAndQuery(t *testing.T) {
	col := newCollector(t)
	ts := newMock(t, tinySupplier(nil), col.srv.URL+"/callbacks")
	no := placeOrder(t, ts, map[string]any{"kind": "REISSUE"})

	got := col.waitFor(t, 1)[0]
	if got.Supplier != "air-a" || got.SupplierOrderNo != no || got.EventSeq != 1 ||
		got.Outcome != "SUCCEEDED" || got.Kind != "REISSUE" {
		t.Fatalf("unexpected callback %+v", got)
	}
	if !strings.HasPrefix(got.InvoiceNo, "AIR-A-") || len(got.InvoiceNo) != len("AIR-A-")+8 {
		t.Fatalf("invoice_no format %q", got.InvoiceNo)
	}
	if p := col.path[0]; p != "/callbacks/air-a" {
		t.Fatalf("callback path %q", p)
	}

	var q struct {
		Found     bool   `json:"found"`
		Outcome   string `json:"outcome"`
		InvoiceNo string `json:"invoice_no"`
	}
	getJSON(t, ts.URL+"/suppliers/air-a/orders/"+no, &q)
	if !q.Found || q.Outcome != "SUCCEEDED" || q.InvoiceNo != got.InvoiceNo {
		t.Fatalf("query %+v", q)
	}
	getJSON(t, ts.URL+"/suppliers/air-a/orders/nope", &q)
	if q.Found {
		t.Fatal("unknown order must not be found")
	}
}

func TestSupplier_DefaultsKindToIssue(t *testing.T) {
	col := newCollector(t)
	ts := newMock(t, tinySupplier(nil), col.srv.URL)
	placeOrder(t, ts, nil)
	if k := col.waitFor(t, 1)[0].Kind; k != "ISSUE" {
		t.Fatalf("kind %q", k)
	}
}

func TestSupplier_OrderFailRateReturns5xx(t *testing.T) {
	ts := newMock(t, tinySupplier(func(s *SupplierCfg) { s.OrderFailRate = 1 }), "http://127.0.0.1:1")
	resp, _ := postJSON(t, ts.URL+"/suppliers/air-a/orders", map[string]any{
		"task_id": "t", "invoice_id": "i", "product_line": "FLIGHT", "title": "x", "tax_no": "y", "amount": "1.00",
	})
	if resp.StatusCode < 500 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestSupplier_FailedOutcomeHasEmptyInvoiceNo(t *testing.T) {
	col := newCollector(t)
	ts := newMock(t, tinySupplier(func(s *SupplierCfg) { s.CallbackFailRate = 1 }), col.srv.URL)
	placeOrder(t, ts, nil)
	got := col.waitFor(t, 1)[0]
	if got.Outcome != "FAILED" || got.InvoiceNo != "" {
		t.Fatalf("callback %+v", got)
	}
}

func TestSupplier_DuplicateCallbacksKeepEventSeq(t *testing.T) {
	col := newCollector(t)
	ts := newMock(t, tinySupplier(func(s *SupplierCfg) { s.DuplicateRate = 1 }), col.srv.URL)
	no := placeOrder(t, ts, nil)

	got := col.waitFor(t, 2)
	for _, g := range got {
		if g.SupplierOrderNo != no || g.EventSeq != got[0].EventSeq || g.InvoiceNo != got[0].InvoiceNo {
			t.Fatalf("duplicates differ: %+v vs %+v", g, got[0])
		}
	}
}

func TestSupplier_OutOfOrderSendsFailedThenSucceeded(t *testing.T) {
	col := newCollector(t)
	ts := newMock(t, tinySupplier(func(s *SupplierCfg) { s.OutOfOrderRate = 1 }), col.srv.URL)
	placeOrder(t, ts, nil)

	got := col.waitFor(t, 2)
	if got[0].EventSeq != 1 || got[0].Outcome != "FAILED" {
		t.Fatalf("first callback %+v", got[0])
	}
	if got[1].EventSeq != 2 || got[1].Outcome != "SUCCEEDED" || got[1].InvoiceNo == "" {
		t.Fatalf("second callback %+v", got[1])
	}
}

func TestSupplier_NoCallback(t *testing.T) {
	col := newCollector(t)
	ts := newMock(t, tinySupplier(func(s *SupplierCfg) {
		s.NoCallbackRate = 1
		s.CallbackDelayMS = [2]int{10, 50}
	}), col.srv.URL)
	no := placeOrder(t, ts, nil)

	time.Sleep(100 * time.Millisecond) // twice the upper delay bound
	if n := len(col.snapshot()); n != 0 {
		t.Fatalf("got %d callbacks, want none", n)
	}
	var q struct {
		Found bool `json:"found"`
	}
	getJSON(t, ts.URL+"/suppliers/air-a/orders/"+no, &q)
	if q.Found {
		t.Fatal("order that never calls back must be reported as not found")
	}
}

func TestSupplier_QueryIsPendingBeforeCallback(t *testing.T) {
	col := newCollector(t)
	ts := newMock(t, tinySupplier(func(s *SupplierCfg) { s.CallbackDelayMS = [2]int{400, 400} }), col.srv.URL)
	no := placeOrder(t, ts, nil)
	var q struct {
		Found   bool   `json:"found"`
		Outcome string `json:"outcome"`
	}
	getJSON(t, ts.URL+"/suppliers/air-a/orders/"+no, &q)
	if !q.Found || q.Outcome != "PENDING" {
		t.Fatalf("query %+v", q)
	}
}

func TestSupplier_RejectsBadRequests(t *testing.T) {
	ts := newMock(t, tinySupplier(nil), "http://127.0.0.1:1")
	good := map[string]any{"task_id": "t", "invoice_id": "i", "product_line": "FLIGHT", "title": "x", "tax_no": "y", "amount": "1.00"}
	with := func(k string, v any) map[string]any {
		m := map[string]any{}
		for kk, vv := range good {
			m[kk] = vv
		}
		m[k] = v
		return m
	}
	cases := []struct {
		url  string
		body map[string]any
		want int
	}{
		{"/suppliers/ghost/orders", good, http.StatusNotFound},
		{"/suppliers/air-a/orders", with("product_line", "TRAIN"), http.StatusBadRequest},
		{"/suppliers/air-a/orders", with("amount", "1.005"), http.StatusBadRequest},
		{"/suppliers/air-a/orders", with("kind", "BOGUS"), http.StatusBadRequest},
	}
	for _, c := range cases {
		resp, raw := postJSON(t, ts.URL+c.url, c.body)
		if resp.StatusCode != c.want {
			t.Errorf("%s %v: status %d (%s), want %d", c.url, c.body, resp.StatusCode, raw, c.want)
		}
	}
}

func TestSink_SlowSuccessRecordsThenTimesOut(t *testing.T) {
	sc := tinySupplier(nil)
	sc.CustomerPush = SinkCfg{SlowSuccessRate: 1, SlowDelayMS: 600}
	ts := newMock(t, sc, "http://127.0.0.1:1")

	client := &http.Client{Timeout: 200 * time.Millisecond}
	raw, _ := json.Marshal(map[string]any{
		"invoice_id": "inv-1", "invoice_version": 1, "invoice_no": "AIR-A-00000001", "amount": "100.10",
	})
	_, err := client.Post(ts.URL+"/customers/c1/push", "application/json", bytes.NewReader(raw))
	if err == nil {
		t.Fatal("expected client timeout")
	}

	var rec struct {
		Count  int `json:"count"`
		Pushes []struct {
			InvoiceID      string `json:"invoice_id"`
			InvoiceVersion int    `json:"invoice_version"`
			InvoiceNo      string `json:"invoice_no"`
			At             string `json:"at"`
		} `json:"pushes"`
	}
	getJSON(t, ts.URL+"/customers/c1/received", &rec)
	if rec.Count != 1 || len(rec.Pushes) != 1 || rec.Pushes[0].InvoiceID != "inv-1" ||
		rec.Pushes[0].InvoiceVersion != 1 || rec.Pushes[0].InvoiceNo != "AIR-A-00000001" || rec.Pushes[0].At == "" {
		t.Fatalf("received %+v", rec)
	}
}

func TestSink_FailRateRecordsNothing(t *testing.T) {
	sc := tinySupplier(nil)
	sc.CustomerPush = SinkCfg{FailRate: 1}
	sc.FinanceWriteback = SinkCfg{FailRate: 1}
	ts := newMock(t, sc, "http://127.0.0.1:1")
	body := map[string]any{"invoice_id": "i", "invoice_version": 1, "invoice_no": "n", "amount": "1.00"}
	for _, p := range []string{"/customers/c1/push", "/finance/writeback"} {
		resp, _ := postJSON(t, ts.URL+p, body)
		if resp.StatusCode < 500 {
			t.Fatalf("%s status %d", p, resp.StatusCode)
		}
	}
	var rec struct{ Count int }
	getJSON(t, ts.URL+"/customers/c1/received", &rec)
	if rec.Count != 0 {
		t.Fatalf("count %d", rec.Count)
	}
}

func TestSink_CountsEveryPushAndWriteback(t *testing.T) {
	ts := newMock(t, tinySupplier(nil), "http://127.0.0.1:1")
	body := map[string]any{"invoice_id": "i", "invoice_version": 2, "invoice_no": "n", "amount": "1.00"}
	for range 3 {
		if resp, _ := postJSON(t, ts.URL+"/customers/c1/push", body); resp.StatusCode != 200 {
			t.Fatalf("push status %d", resp.StatusCode)
		}
	}
	if resp, _ := postJSON(t, ts.URL+"/finance/writeback", body); resp.StatusCode != 200 {
		t.Fatalf("writeback status %d", resp.StatusCode)
	}
	var rec struct{ Count int }
	getJSON(t, ts.URL+"/customers/c1/received", &rec)
	if rec.Count != 3 {
		t.Fatalf("customer count %d", rec.Count)
	}
	getJSON(t, ts.URL+"/customers/other/received", &rec)
	if rec.Count != 0 {
		t.Fatalf("other customer count %d", rec.Count)
	}
	getJSON(t, ts.URL+"/finance/received", &rec)
	if rec.Count != 1 {
		t.Fatalf("finance count %d", rec.Count)
	}
}

type verifyResp struct {
	Found  bool   `json:"found"`
	Amount string `json:"amount"`
	Title  string `json:"title"`
	Status string `json:"status"`
}

func issueAndVerify(t *testing.T, sc Scenario, extra map[string]any) (verifyResp, int) {
	t.Helper()
	col := newCollector(t)
	ts := newMock(t, sc, col.srv.URL)
	placeOrder(t, ts, extra)
	inv := col.waitFor(t, 1)[0].InvoiceNo
	var v verifyResp
	code := getJSON(t, ts.URL+"/verify/FLIGHT/"+inv, &v)
	return v, code
}

func TestVerifyData_MatchesOrderByDefault(t *testing.T) {
	v, _ := issueAndVerify(t, tinySupplier(nil), nil)
	if !v.Found || v.Amount != "100.10" || v.Title != "ACME Ltd" || v.Status != "issued" {
		t.Fatalf("verify %+v", v)
	}
	v, _ = issueAndVerify(t, tinySupplier(nil), map[string]any{"kind": "RED_FLUSH"})
	if v.Status != "red_flushed" {
		t.Fatalf("status %q", v.Status)
	}
}

func TestVerifyData_MismatchRates(t *testing.T) {
	sc := tinySupplier(nil)
	sc.Verify.AmountMismatchRate = 1
	v, _ := issueAndVerify(t, sc, nil)
	if !v.Found || v.Amount == "100.10" || v.Title != "ACME Ltd" {
		t.Fatalf("amount mismatch %+v", v)
	}

	sc = tinySupplier(nil)
	sc.Verify.TitleMismatchRate = 1
	v, _ = issueAndVerify(t, sc, nil)
	if v.Title == "ACME Ltd" || v.Amount != "100.10" {
		t.Fatalf("title mismatch %+v", v)
	}

	sc = tinySupplier(nil)
	sc.Verify.StatusMismatchRate = 1
	v, _ = issueAndVerify(t, sc, nil)
	if v.Status == "issued" {
		t.Fatalf("status mismatch %+v", v)
	}

	sc = tinySupplier(nil)
	sc.Verify.MissingRate = 1
	v, _ = issueAndVerify(t, sc, nil)
	if v.Found {
		t.Fatalf("missing %+v", v)
	}
}

func TestVerifyData_UnknownInvoice(t *testing.T) {
	ts := newMock(t, tinySupplier(nil), "http://127.0.0.1:1")
	var v verifyResp
	if code := getJSON(t, ts.URL+"/verify/FLIGHT/AIR-A-99999999", &v); code != 200 || v.Found {
		t.Fatalf("code %d %+v", code, v)
	}
}

func TestHealthz(t *testing.T) {
	ts := newMock(t, tinySupplier(nil), "http://127.0.0.1:1")
	if code := getJSON(t, ts.URL+"/healthz", nil); code != 200 {
		t.Fatalf("code %d", code)
	}
}

func TestClose_StopsPendingCallbacks(t *testing.T) {
	col := newCollector(t)
	m := NewServer(tinySupplier(func(s *SupplierCfg) { s.CallbackDelayMS = [2]int{300, 300} }), col.srv.URL)
	ts := httptest.NewServer(m)
	defer ts.Close()
	placeOrder(t, ts, nil)
	m.Close()
	time.Sleep(500 * time.Millisecond)
	if n := len(col.snapshot()); n != 0 {
		t.Fatalf("got %d callbacks after Close", n)
	}
}
