package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	invoicev1 "github.com/mafuky/invoice-hub/gen/invoicehub/invoice/v1"
	providerv1 "github.com/mafuky/invoice-hub/gen/invoicehub/provider/v1"
	settlementv1 "github.com/mafuky/invoice-hub/gen/invoicehub/settlement/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type invoiceRoutesFake struct {
	invoicev1.InvoiceServiceClient
	t *testing.T
}

func (f invoiceRoutesFake) Get(ctx context.Context, r *invoicev1.GetRequest, _ ...grpc.CallOption) (*invoicev1.Invoice, error) {
	if r.InvoiceId != 42 {
		f.t.Fatalf("invoice id=%d", r.InvoiceId)
	}
	return &invoicev1.Invoice{InvoiceId: 42, Amount: "100.1", ProductLine: invoicev1.ProductLine_PRODUCT_LINE_TRAIN, Status: "closed"}, nil
}
func (f invoiceRoutesFake) List(ctx context.Context, r *invoicev1.ListRequest, _ ...grpc.CallOption) (*invoicev1.ListReply, error) {
	if r.Status != "closed" || r.PageSize != 25 || r.PageToken != "opaque" {
		f.t.Fatalf("list request=%v", r)
	}
	return &invoicev1.ListReply{Invoices: []*invoicev1.Invoice{{InvoiceId: 42, Amount: "1999.99"}}, NextPageToken: "next"}, nil
}
func (f invoiceRoutesFake) RedFlush(ctx context.Context, r *invoicev1.RedFlushRequest, _ ...grpc.CallOption) (*invoicev1.CommandReply, error) {
	if r.InvoiceId != 42 || r.Reason != "refund" {
		f.t.Fatalf("red flush=%v", r)
	}
	return &invoicev1.CommandReply{InvoiceId: 42, CommandId: 8, Status: "created"}, nil
}
func (f invoiceRoutesFake) Reissue(ctx context.Context, r *invoicev1.ReissueRequest, _ ...grpc.CallOption) (*invoicev1.CommandReply, error) {
	if r.InvoiceId != 42 || r.Reason != "changed itinerary" {
		f.t.Fatalf("reissue=%v", r)
	}
	return &invoicev1.CommandReply{InvoiceId: 42, CommandId: 9, Status: "created"}, nil
}
func (f invoiceRoutesFake) Supplement(ctx context.Context, r *invoicev1.SupplementRequest, _ ...grpc.CallOption) (*invoicev1.CommandReply, error) {
	if r.BizOrderNo != "BIZ-20261025-0001" || r.ProductLine != invoicev1.ProductLine_PRODUCT_LINE_FLIGHT || r.Amount != "100.10" || r.CustomerId != "demo-customer" || r.Title != "Example Test Company" || r.TaxNo != "FAKE-TAX-0001" {
		f.t.Fatalf("supplement=%v", r)
	}
	return &invoicev1.CommandReply{InvoiceId: 43, CommandId: 10, Status: "created"}, nil
}
func TestRouter_InvoiceReadsAndCommands(t *testing.T) {
	h := NewRouter(invoiceRoutesFake{t: t}, nil, nil)
	for _, tc := range []struct{ method, path, body, want string }{
		{"GET", "/invoices/42", "", `"amount":"100.10"`},
		{"GET", "/invoices?status=closed&page_size=25&page_token=opaque", "", `"next_page_token":"next"`},
		{"POST", "/invoices/42/red-flush", `{"reason":"refund"}`, `{"command_id":8,"invoice_id":42,"status":"created"}`},
		{"POST", "/invoices/42/reissue", `{"reason":"changed itinerary"}`, `{"command_id":9,"invoice_id":42,"status":"created"}`},
		{"POST", "/invoices/supplement", applyJSON, `{"command_id":10,"invoice_id":43,"status":"created"}`},
	} {
		w := request(h, tc.method, tc.path, tc.body, "")
		if w.Code != 200 || !strings.Contains(w.Body.String(), tc.want) {
			t.Fatalf("%s: status=%d body=%s", tc.path, w.Code, w.Body)
		}
	}
}

type settlementFake struct {
	settlementv1.SettlementServiceClient
	t      *testing.T
	called string
}

func (f *settlementFake) result(ctx context.Context, id int64, method string) (*settlementv1.Settlement, error) {
	if id != 17 {
		f.t.Fatalf("settlement id=%d", id)
	}
	md, _ := metadata.FromOutgoingContext(ctx)
	if strings.Join(md.Get("x-trace-id"), "") != "settle-trace" {
		f.t.Fatal("settlement trace missing")
	}
	f.called = method
	return &settlementv1.Settlement{SettlementId: 17, Amount: "100.1", Status: "pending_settle", Items: []*settlementv1.SettlementItem{{InvoiceId: 42, Amount: "100.10"}}}, nil
}
func (f *settlementFake) Get(ctx context.Context, r *settlementv1.GetRequest, _ ...grpc.CallOption) (*settlementv1.Settlement, error) {
	return f.result(ctx, r.SettlementId, "get")
}
func (f *settlementFake) Calculate(ctx context.Context, r *settlementv1.IDRequest, _ ...grpc.CallOption) (*settlementv1.Settlement, error) {
	return f.result(ctx, r.SettlementId, "calculate")
}
func (f *settlementFake) SubmitReview(ctx context.Context, r *settlementv1.IDRequest, _ ...grpc.CallOption) (*settlementv1.Settlement, error) {
	return f.result(ctx, r.SettlementId, "submit")
}
func (f *settlementFake) Approve(ctx context.Context, r *settlementv1.IDRequest, _ ...grpc.CallOption) (*settlementv1.Settlement, error) {
	return f.result(ctx, r.SettlementId, "approve")
}
func (f *settlementFake) Pay(ctx context.Context, r *settlementv1.IDRequest, _ ...grpc.CallOption) (*settlementv1.Settlement, error) {
	return f.result(ctx, r.SettlementId, "pay")
}
func (f *settlementFake) Reject(ctx context.Context, r *settlementv1.RejectRequest, _ ...grpc.CallOption) (*settlementv1.Settlement, error) {
	if r.Reason != "amount incorrect" {
		f.t.Fatal(r)
	}
	return f.result(ctx, r.SettlementId, "reject")
}
func TestRouter_SettlementReadsAndActions(t *testing.T) {
	f := &settlementFake{t: t}
	h := NewRouter(nil, nil, f)
	for _, action := range []string{"get", "calculate", "submit", "approve", "reject", "pay"} {
		method, path, body := "POST", "/settlements/17/"+action, ""
		if action == "get" {
			method, path = "GET", "/settlements/17"
		}
		if action == "reject" {
			body = `{"reason":"amount incorrect"}`
		}
		w := request(h, method, path, body, "settle-trace")
		if w.Code != 200 || f.called != action || strings.Count(w.Body.String(), `"amount":"100.10"`) != 2 {
			t.Fatalf("action=%s called=%s status=%d body=%s", action, f.called, w.Code, w.Body)
		}
	}
}
func TestRouter_InvalidIDsHealthAndErrors(t *testing.T) {
	h := NewRouter(nil, nil, nil)
	for _, path := range []string{"/invoices/0", "/invoices/nope", "/settlements/-1", "/invoices?page_size=nope"} {
		if w := request(h, "GET", path, "", ""); w.Code != 400 {
			t.Fatalf("%s: %d", path, w.Code)
		}
	}
	for _, tc := range []struct {
		method, path string
		code         int
	}{{"GET", "/healthz", 200}, {"GET", "/missing", 404}, {"DELETE", "/invoices", 405}} {
		w := request(h, tc.method, tc.path, "", "")
		if w.Code != tc.code || w.Header().Get("X-Trace-Id") == "" {
			t.Fatalf("%s: %d", tc.path, w.Code)
		}
	}
}

type invoiceFake struct {
	invoicev1.InvoiceServiceClient
	apply func(context.Context, *invoicev1.ApplyRequest) (*invoicev1.ApplyReply, error)
}

func (f invoiceFake) Apply(ctx context.Context, r *invoicev1.ApplyRequest, _ ...grpc.CallOption) (*invoicev1.ApplyReply, error) {
	return f.apply(ctx, r)
}

type providerFake struct {
	callback func(context.Context, *providerv1.CallbackRequest) (*providerv1.CallbackReply, error)
}

func (f providerFake) HandleCallback(ctx context.Context, r *providerv1.CallbackRequest, _ ...grpc.CallOption) (*providerv1.CallbackReply, error) {
	return f.callback(ctx, r)
}

const applyJSON = `{"biz_order_no":"BIZ-20261025-0001","product_line":"FLIGHT","title":"Example Test Company","tax_no":"FAKE-TAX-0001","amount":"100.10","customer_id":"demo-customer"}`
const callbackJSON = `{ "event_seq": 7, "supplier_order_no":"fake-order-1", "outcome":"SUCCEEDED", "invoice_no":"FAKE-INV-1" }`

func request(h http.Handler, method, path, body, trace string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if trace != "" {
		r.Header.Set("X-Trace-Id", trace)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestRouter_CallbackUnknownOrderIs404(t *testing.T) {
	h := NewRouter(nil, providerFake{callback: func(_ context.Context, r *providerv1.CallbackRequest) (*providerv1.CallbackReply, error) {
		if r.Supplier != "air-a" || r.EventSeq != 7 || r.SupplierOrderNo != "fake-order-1" || r.InvoiceNo != "FAKE-INV-1" || r.Outcome != providerv1.CallbackOutcome_CALLBACK_OUTCOME_SUCCEEDED || r.RawJson != callbackJSON {
			t.Fatalf("callback changed: %v", r)
		}
		return nil, status.Error(codes.NotFound, "unknown order")
	}}, nil)
	w := request(h, "POST", "/callbacks/air-a", callbackJSON, "")
	if w.Code != 404 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body)
	}
}
func TestRouter_UnimplementedIs501WithLessonHint(t *testing.T) {
	h := NewRouter(invoiceFake{apply: func(context.Context, *invoicev1.ApplyRequest) (*invoicev1.ApplyReply, error) {
		return nil, status.Error(codes.Unimplemented, "lesson 2")
	}}, nil, nil)
	w := request(h, "POST", "/invoices", applyJSON, "")
	if w.Code != 501 || !strings.Contains(w.Body.String(), "lesson 2") {
		t.Fatalf("status=%d body=%s", w.Code, w.Body)
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body["error"] == "" {
		t.Fatalf("not an error object: %s", w.Body)
	}
}
func TestRouter_TraceIDPropagates(t *testing.T) {
	for _, trace := range []string{"t-1", ""} {
		t.Run(trace, func(t *testing.T) {
			var received string
			h := NewRouter(invoiceFake{apply: func(ctx context.Context, r *invoicev1.ApplyRequest) (*invoicev1.ApplyReply, error) {
				md, _ := metadata.FromOutgoingContext(ctx)
				received = strings.Join(md.Get("x-trace-id"), "")
				if r.Amount != "100.10" || r.ProductLine != invoicev1.ProductLine_PRODUCT_LINE_FLIGHT {
					t.Fatalf("request=%v", r)
				}
				return &invoicev1.ApplyReply{InvoiceId: 42}, nil
			}}, nil, nil)
			w := request(h, "POST", "/invoices", applyJSON, trace)
			if w.Code != 202 || received == "" || w.Header().Get("X-Trace-Id") != received || (trace != "" && received != trace) {
				t.Fatalf("status=%d trace=%q header=%q", w.Code, received, w.Header().Get("X-Trace-Id"))
			}
			if w.Body.String() != `{"existed":false,"invoice_id":42}` {
				t.Fatalf("reply=%s", w.Body)
			}
		})
	}
}
func TestRouter_GRPCErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		code codes.Code
		http int
	}{{codes.Unimplemented, 501}, {codes.NotFound, 404}, {codes.InvalidArgument, 400}, {codes.FailedPrecondition, 409}, {codes.AlreadyExists, 409}, {codes.Unavailable, 503}, {codes.DeadlineExceeded, 504}, {codes.Internal, 500}, {codes.Canceled, 500}, {codes.Unknown, 500}} {
		t.Run(tc.code.String(), func(t *testing.T) {
			h := NewRouter(invoiceFake{apply: func(context.Context, *invoicev1.ApplyRequest) (*invoicev1.ApplyReply, error) {
				return nil, status.Error(tc.code, "invoice error")
			}}, nil, nil)
			w := request(h, "POST", "/invoices", applyJSON, "")
			if w.Code != tc.http {
				t.Fatalf("status=%d want=%d", w.Code, tc.http)
			}
		})
	}
}
func TestRouter_CallbackAcceptedDuplicateAndBusy(t *testing.T) {
	for _, tc := range []struct {
		result providerv1.CallbackResult
		err    error
		want   int
	}{{providerv1.CallbackResult_CALLBACK_RESULT_ACCEPTED, nil, 200}, {providerv1.CallbackResult_CALLBACK_RESULT_DUPLICATE, nil, 200}, {providerv1.CallbackResult_CALLBACK_RESULT_UNKNOWN_ORDER, nil, 404}, {0, status.Error(codes.Unavailable, "callback busy"), 503}, {0, nil, 500}} {
		h := NewRouter(nil, providerFake{callback: func(context.Context, *providerv1.CallbackRequest) (*providerv1.CallbackReply, error) {
			return &providerv1.CallbackReply{Result: tc.result}, tc.err
		}}, nil)
		w := request(h, "POST", "/callbacks/air-a", callbackJSON, "")
		if w.Code != tc.want {
			t.Fatalf("result=%v status=%d want=%d", tc.result, w.Code, tc.want)
		}
	}
}
func TestRouter_RejectsInvalidAmountAndJSON(t *testing.T) {
	h := NewRouter(invoiceFake{apply: func(context.Context, *invoicev1.ApplyRequest) (*invoicev1.ApplyReply, error) {
		t.Fatal("invalid invoice reached gRPC")
		return nil, nil
	}}, nil, nil)
	for _, body := range []string{`{`, strings.Replace(applyJSON, `"100.10"`, `100.10`, 1), strings.Replace(applyJSON, `"100.10"`, `"100.101"`, 1), strings.Replace(applyJSON, `"100.10"`, `null`, 1), applyJSON + ` {}`} {
		w := request(h, "POST", "/invoices", body, "")
		if w.Code != 400 {
			t.Fatalf("body=%s status=%d", body, w.Code)
		}
	}
}
