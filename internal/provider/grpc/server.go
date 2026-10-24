// Package providergrpc serves invoicehub.provider.v1.ProviderService.
package providergrpc

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	providerv1 "github.com/mafuky/invoice-hub/gen/invoicehub/provider/v1"
	"github.com/mafuky/invoice-hub/internal/platform/grpcx"
	"github.com/mafuky/invoice-hub/internal/provider/callback"
	"github.com/mafuky/invoice-hub/internal/provider/client"
	"github.com/mafuky/invoice-hub/internal/provider/store"
)

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

// NewGRPCServer returns a grpc.Server with s registered behind the shared interceptors.
func NewGRPCServer(s *Server) *grpc.Server {
	gs := grpcx.NewServer("provider", mapErr)
	providerv1.RegisterProviderServiceServer(gs, s)
	return gs
}

func mapErr(ctx context.Context, method string, err error) (error, bool) {
	switch {
	case errors.Is(err, callback.ErrBusy):
		return status.Error(codes.Unavailable, "order is being processed, retry"), true
	case errors.Is(err, client.ErrSupplier):
		slog.WarnContext(ctx, "supplier unavailable", "method", method, "err", err)
		return status.Error(codes.Unavailable, "supplier unavailable"), true
	}
	return nil, false
}

// HandleCallback stores the raw payload first, so it is kept even when the
// order is unknown or the handler fails, then lets the handler apply it.
func (s *Server) HandleCallback(ctx context.Context, req *providerv1.CallbackRequest) (*providerv1.CallbackReply, error) {
	outcome, ok := outcomeName(req.GetOutcome())
	if req.GetSupplier() == "" || req.GetSupplierOrderNo() == "" || !ok || req.GetEventSeq() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "supplier, supplier_order_no, a known outcome and a positive event_seq are required")
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
	return nil, errors.New("callback handler returned an unknown result")
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
	return &providerv1.QueryOrderReply{Found: true, Outcome: orderOutcome(ctx, res.Outcome), InvoiceNo: res.InvoiceNo}, nil
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

func orderOutcome(ctx context.Context, s string) providerv1.OrderOutcome {
	switch s {
	case "PENDING":
		return providerv1.OrderOutcome_ORDER_OUTCOME_PENDING
	case "SUCCEEDED":
		return providerv1.OrderOutcome_ORDER_OUTCOME_SUCCEEDED
	case "FAILED":
		return providerv1.OrderOutcome_ORDER_OUTCOME_FAILED
	}
	slog.WarnContext(ctx, "supplier reported an unknown order outcome", "outcome", s)
	return providerv1.OrderOutcome_ORDER_OUTCOME_UNSPECIFIED
}
