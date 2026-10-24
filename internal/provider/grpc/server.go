// Package providergrpc serves invoicehub.provider.v1.ProviderService.
package providergrpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	providerv1 "github.com/mafuky/invoice-hub/gen/invoicehub/provider/v1"
	"github.com/mafuky/invoice-hub/internal/platform/logx"
	"github.com/mafuky/invoice-hub/internal/provider/callback"
	"github.com/mafuky/invoice-hub/internal/provider/client"
	"github.com/mafuky/invoice-hub/internal/provider/store"
)

const traceHeader = "x-trace-id"

// CallbackHandler applies a supplier callback to its order.
type CallbackHandler interface {
	Handle(ctx context.Context, cb callback.Callback) (callback.Result, error)
}

// RawSink stores a callback payload as received. inserted=false means the
// same payload (supplier order and event_seq) was stored before.
type RawSink interface {
	Store(ctx context.Context, cb store.RawCallback) (inserted bool, err error)
}

// Orders reads the local provider orders.
type Orders interface {
	OrderByTask(ctx context.Context, taskID int64) (store.Order, error)
}

// Supplier asks a supplier about an order.
type Supplier interface {
	QueryOrder(ctx context.Context, supplier, supplierOrderNo string) (client.QueryResult, error)
}

type Server struct {
	providerv1.UnimplementedProviderServiceServer
	handler  CallbackHandler
	raw      RawSink
	orders   Orders
	supplier Supplier
}

func NewServer(handler CallbackHandler, raw RawSink, orders Orders, supplier Supplier) *Server {
	return &Server{handler: handler, raw: raw, orders: orders, supplier: supplier}
}

// NewGRPCServer returns a grpc.Server with s registered behind the trace and error-mapping interceptor.
func NewGRPCServer(s *Server) *grpc.Server {
	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(interceptor, recoverPanic))
	providerv1.RegisterProviderServiceServer(gs, s)
	return gs
}

func recoverPanic(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (resp any, err error) {
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "provider rpc panicked", "method", info.FullMethod, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			resp, err = nil, status.Error(codes.Internal, "internal error")
		}
	}()
	return h(ctx, req)
}

var lessonRe = regexp.MustCompile(`lesson (\d+)`)

func interceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(traceHeader); len(v) > 0 && v[0] != "" {
			ctx = logx.WithTrace(ctx, v[0])
		}
	}
	resp, err := h(ctx, req)
	if err == nil {
		return resp, nil
	}
	return nil, toStatus(ctx, info.FullMethod, err)
}

func toStatus(ctx context.Context, method string, err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, errors.ErrUnsupported):
		msg := "not implemented yet"
		if m := lessonRe.FindStringSubmatch(err.Error()); m != nil {
			msg += " (lesson " + m[1] + ")"
		}
		return status.Error(codes.Unimplemented, msg)
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, client.ErrSupplier):
		slog.WarnContext(ctx, "supplier unavailable", "method", method, "err", err)
		return status.Error(codes.Unavailable, "supplier unavailable")
	}
	slog.ErrorContext(ctx, "provider rpc failed", "method", method, "err", err)
	return status.Error(codes.Internal, "internal error")
}

// HandleCallback stores the raw payload first, so it is kept even when the
// order is unknown or the handler fails, then lets the handler apply it.
func (s *Server) HandleCallback(ctx context.Context, req *providerv1.CallbackRequest) (*providerv1.CallbackReply, error) {
	outcome, ok := outcomeName(req.GetOutcome())
	if req.GetSupplier() == "" || req.GetSupplierOrderNo() == "" || !ok {
		return nil, status.Error(codes.InvalidArgument, "supplier, supplier_order_no and a known outcome are required")
	}
	inserted, err := s.raw.Store(ctx, store.RawCallback{
		Supplier: req.GetSupplier(), SupplierOrderNo: req.GetSupplierOrderNo(), EventSeq: req.GetEventSeq(),
		Outcome: string(outcome), InvoiceNo: req.GetInvoiceNo(), RawJSON: req.GetRawJson(),
	})
	if err != nil {
		return nil, err
	}
	res, err := s.handler.Handle(ctx, callback.Callback{
		Supplier: req.GetSupplier(), SupplierOrderNo: req.GetSupplierOrderNo(), EventSeq: req.GetEventSeq(),
		Outcome: outcome, InvoiceNo: req.GetInvoiceNo(), RawJSON: req.GetRawJson(), RawInserted: inserted,
	})
	if err != nil {
		return nil, err
	}
	switch res {
	case callback.Accepted:
		return &providerv1.CallbackReply{Result: providerv1.CallbackResult_CALLBACK_RESULT_ACCEPTED}, nil
	case callback.Duplicate:
		return &providerv1.CallbackReply{Result: providerv1.CallbackResult_CALLBACK_RESULT_DUPLICATE}, nil
	case callback.UnknownOrder:
		return nil, status.Errorf(codes.NotFound, "unknown supplier order %s/%s", req.GetSupplier(), req.GetSupplierOrderNo())
	}
	return nil, fmt.Errorf("callback handler returned %v", res)
}

// QueryOrder reports what the supplier says about the order of a task.
func (s *Server) QueryOrder(ctx context.Context, req *providerv1.QueryOrderRequest) (*providerv1.QueryOrderReply, error) {
	o, err := s.orders.OrderByTask(ctx, req.GetTaskId())
	if errors.Is(err, store.ErrNotFound) {
		return &providerv1.QueryOrderReply{Found: false, Outcome: providerv1.OrderOutcome_ORDER_OUTCOME_UNSPECIFIED}, nil
	}
	if err != nil {
		return nil, err
	}
	res, err := s.supplier.QueryOrder(ctx, o.Supplier, o.SupplierOrderNo)
	if err != nil {
		return nil, err
	}
	if !res.Found {
		return &providerv1.QueryOrderReply{Found: false}, nil
	}
	return &providerv1.QueryOrderReply{Found: true, Outcome: orderOutcome(res.Outcome), InvoiceNo: res.InvoiceNo}, nil
}

func outcomeName(o providerv1.CallbackOutcome) (callback.Outcome, bool) {
	switch o {
	case providerv1.CallbackOutcome_CALLBACK_OUTCOME_SUCCEEDED:
		return callback.OutcomeSucceeded, true
	case providerv1.CallbackOutcome_CALLBACK_OUTCOME_FAILED:
		return callback.OutcomeFailed, true
	}
	return "", false
}

func orderOutcome(s string) providerv1.OrderOutcome {
	switch s {
	case "PENDING":
		return providerv1.OrderOutcome_ORDER_OUTCOME_PENDING
	case "SUCCEEDED":
		return providerv1.OrderOutcome_ORDER_OUTCOME_SUCCEEDED
	case "FAILED":
		return providerv1.OrderOutcome_ORDER_OUTCOME_FAILED
	}
	return providerv1.OrderOutcome_ORDER_OUTCOME_UNSPECIFIED
}
