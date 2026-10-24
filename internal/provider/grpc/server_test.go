package providergrpc_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	providerv1 "github.com/mafuky/invoice-hub/gen/invoicehub/provider/v1"
	"github.com/mafuky/invoice-hub/internal/provider/callback"
	"github.com/mafuky/invoice-hub/internal/provider/client"
	providergrpc "github.com/mafuky/invoice-hub/internal/provider/grpc"
	"github.com/mafuky/invoice-hub/internal/provider/store"
)

type fakeHandler struct {
	result callback.Result
	err    error
	got    []callback.Callback
}

func (f *fakeHandler) Handle(_ context.Context, cb callback.Callback) (callback.Result, error) {
	f.got = append(f.got, cb)
	return f.result, f.err
}

type fakeSink struct {
	mu     sync.Mutex
	stored []store.RawCallback
	seen   map[string]bool
}

func (f *fakeSink) Store(_ context.Context, cb store.RawCallback) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.seen == nil {
		f.seen = map[string]bool{}
	}
	key := fmt.Sprintf("%s/%s/%d", cb.Supplier, cb.SupplierOrderNo, cb.EventSeq)
	if f.seen[key] {
		return false, nil
	}
	f.seen[key] = true
	f.stored = append(f.stored, cb)
	return true, nil
}

type fakeOrders map[int64]store.Order

func (f fakeOrders) OrderByTask(_ context.Context, taskID int64) (store.Order, error) {
	if o, ok := f[taskID]; ok {
		return o, nil
	}
	return store.Order{}, fmt.Errorf("task %d: %w", taskID, store.ErrNotFound)
}

func start(t *testing.T, h providergrpc.CallbackHandler, raw providergrpc.RawSink, orders providergrpc.Orders, sup providergrpc.Supplier) providerv1.ProviderServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := providergrpc.NewGRPCServer(providergrpc.NewServer(h, raw, orders, sup))
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return providerv1.NewProviderServiceClient(conn)
}

func callbackRequest() *providerv1.CallbackRequest {
	return &providerv1.CallbackRequest{
		Supplier:        "air-a",
		SupplierOrderNo: "AIR-A-ORD-000999",
		EventSeq:        1,
		Outcome:         providerv1.CallbackOutcome_CALLBACK_OUTCOME_SUCCEEDED,
		InvoiceNo:       "AIR-A-12345678",
		RawJson:         `{"supplier":"air-a","supplier_order_no":"AIR-A-ORD-000999","event_seq":1}`,
	}
}

func TestHandleCallback_UnknownOrder_Returns404AndStoresRaw(t *testing.T) {
	sink := &fakeSink{}
	h := &fakeHandler{result: callback.UnknownOrder}
	c := start(t, h, sink, fakeOrders{}, nil)

	_, err := c.HandleCallback(context.Background(), callbackRequest())
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("code = %v, want NotFound (err: %v)", got, err)
	}
	if len(sink.stored) != 1 {
		t.Fatalf("raw payload stored %d times, want 1", len(sink.stored))
	}
	if sink.stored[0].RawJSON != callbackRequest().RawJson {
		t.Fatalf("stored payload %q differs from the one received", sink.stored[0].RawJSON)
	}
	if len(h.got) != 1 || !h.got[0].RawInserted {
		t.Fatalf("handler saw %+v, want one call with RawInserted=true", h.got)
	}
}

func TestHandleCallback_RepeatedPayload_PassesRawInsertedFalse(t *testing.T) {
	sink := &fakeSink{}
	h := &fakeHandler{result: callback.Duplicate}
	c := start(t, h, sink, fakeOrders{}, nil)

	for range 2 {
		reply, err := c.HandleCallback(context.Background(), callbackRequest())
		if err != nil {
			t.Fatal(err)
		}
		if reply.GetResult() != providerv1.CallbackResult_CALLBACK_RESULT_DUPLICATE {
			t.Fatalf("result = %v, want DUPLICATE", reply.GetResult())
		}
	}
	if len(sink.stored) != 1 {
		t.Fatalf("raw payload stored %d times, want 1", len(sink.stored))
	}
	if len(h.got) != 2 || !h.got[0].RawInserted || h.got[1].RawInserted {
		t.Fatalf("RawInserted flags = %v %v, want true then false", h.got[0].RawInserted, h.got[1].RawInserted)
	}
}

func TestHandleCallback_UnfinishedLesson_StoresRawAndReturnsUnimplemented(t *testing.T) {
	sink := &fakeSink{}
	h := &fakeHandler{err: fmt.Errorf("lesson 5: %w", errors.ErrUnsupported)}
	c := start(t, h, sink, fakeOrders{}, nil)

	_, err := c.HandleCallback(context.Background(), callbackRequest())
	st, _ := status.FromError(err)
	if st.Code() != codes.Unimplemented || !strings.Contains(st.Message(), "lesson 5") {
		t.Fatalf("got %v %q, want Unimplemented naming lesson 5", st.Code(), st.Message())
	}
	if len(sink.stored) != 1 {
		t.Fatalf("raw payload stored %d times, want 1", len(sink.stored))
	}
}

func TestHandleCallback_PanicInHandler_ReturnsInternal(t *testing.T) {
	c := start(t, panicHandler{}, &fakeSink{}, fakeOrders{}, nil)
	_, err := c.HandleCallback(context.Background(), callbackRequest())
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("code = %v, want Internal", got)
	}
}

type panicHandler struct{}

func (panicHandler) Handle(context.Context, callback.Callback) (callback.Result, error) {
	panic("handler blew up")
}

func TestHandleCallback_MissingFields_InvalidArgument(t *testing.T) {
	sink := &fakeSink{}
	c := start(t, &fakeHandler{result: callback.Accepted}, sink, fakeOrders{}, nil)
	req := callbackRequest()
	req.SupplierOrderNo = ""
	_, err := c.HandleCallback(context.Background(), req)
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", got)
	}
	if len(sink.stored) != 0 {
		t.Fatal("a malformed request was stored")
	}
}

func TestQueryOrder_LocalAndMock(t *testing.T) {
	var asked string
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "AIR-A-ORD-000001"):
			fmt.Fprint(w, `{"found":true,"outcome":"SUCCEEDED","invoice_no":"AIR-A-12345678"}`)
		default:
			fmt.Fprint(w, `{"found":false,"outcome":"","invoice_no":""}`)
		}
	}))
	t.Cleanup(mock.Close)

	orders := fakeOrders{
		11: {TaskID: 11, Supplier: "air-a", SupplierOrderNo: "AIR-A-ORD-000001"},
		12: {TaskID: 12, Supplier: "air-a", SupplierOrderNo: "AIR-A-ORD-000002"},
	}
	c := start(t, &fakeHandler{}, &fakeSink{}, orders, client.NewMock(mock.URL))

	reply, err := c.QueryOrder(context.Background(), &providerv1.QueryOrderRequest{TaskId: 11})
	if err != nil {
		t.Fatal(err)
	}
	if !reply.GetFound() || reply.GetOutcome() != providerv1.OrderOutcome_ORDER_OUTCOME_SUCCEEDED || reply.GetInvoiceNo() != "AIR-A-12345678" {
		t.Fatalf("reply = %v, want found SUCCEEDED AIR-A-12345678", reply)
	}
	if asked != "/suppliers/air-a/orders/AIR-A-ORD-000001" {
		t.Fatalf("mock asked at %q", asked)
	}

	reply, err = c.QueryOrder(context.Background(), &providerv1.QueryOrderRequest{TaskId: 12})
	if err != nil || reply.GetFound() {
		t.Fatalf("supplier that never heard of the order: found=%v err=%v, want found=false", reply.GetFound(), err)
	}

	reply, err = c.QueryOrder(context.Background(), &providerv1.QueryOrderRequest{TaskId: 99})
	if err != nil || reply.GetFound() {
		t.Fatalf("task without an order: found=%v err=%v, want found=false", reply.GetFound(), err)
	}
}

func TestQueryOrder_SupplierDown_Unavailable(t *testing.T) {
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	t.Cleanup(mock.Close)
	orders := fakeOrders{11: {TaskID: 11, Supplier: "air-a", SupplierOrderNo: "AIR-A-ORD-000001"}}
	c := start(t, &fakeHandler{}, &fakeSink{}, orders, client.NewMock(mock.URL))

	_, err := c.QueryOrder(context.Background(), &providerv1.QueryOrderRequest{TaskId: 11})
	if got := status.Code(err); got != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable", got)
	}
}
