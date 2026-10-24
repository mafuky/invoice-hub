// Package settlementgrpc serves invoicehub.settlement.v1.SettlementService.
package settlementgrpc

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	settlementv1 "github.com/mafuky/invoice-hub/gen/invoicehub/settlement/v1"
	"github.com/mafuky/invoice-hub/internal/platform/grpcx"
	"github.com/mafuky/invoice-hub/internal/settlement/domain"
	"github.com/mafuky/invoice-hub/internal/settlement/store"
)

// Commands is the write side of the settlement service.
type Commands interface {
	Calculate(ctx context.Context, id int64) error
	SubmitReview(ctx context.Context, id int64) error
	Approve(ctx context.Context, id int64) error
	Reject(ctx context.Context, id int64, reason string) error
	Pay(ctx context.Context, id int64) error
}

// Reader is the read side of the settlement service.
type Reader interface {
	GetSettlement(ctx context.Context, id int64) (domain.Settlement, error)
	ListSettlements(ctx context.Context, status string) ([]domain.Settlement, error)
	ListItems(ctx context.Context, settlementID int64) ([]store.Item, error)
}

type Server struct {
	settlementv1.UnimplementedSettlementServiceServer
	cmds   Commands
	reader Reader
}

func NewServer(cmds Commands, reader Reader) *Server {
	return &Server{cmds: cmds, reader: reader}
}

// NewGRPCServer returns a grpc.Server with s registered behind the shared interceptors.
func NewGRPCServer(s *Server) *grpc.Server {
	gs := grpcx.NewServer("settlement", mapErr)
	settlementv1.RegisterSettlementServiceServer(gs, s)
	return gs
}

func mapErr(_ context.Context, _ string, err error) (error, bool) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, "settlement not found"), true
	case errors.Is(err, domain.ErrRefused):
		return status.Error(codes.FailedPrecondition, "settlement is not in a state that allows this"), true
	}
	return nil, false
}

func (s *Server) Get(ctx context.Context, req *settlementv1.GetRequest) (*settlementv1.Settlement, error) {
	return s.load(ctx, req.GetSettlementId())
}

func (s *Server) List(ctx context.Context, req *settlementv1.ListRequest) (*settlementv1.ListReply, error) {
	if st := req.GetStatus(); st != "" && !domain.KnownSettlementStatus(domain.SettlementStatus(st)) {
		return nil, status.Errorf(codes.InvalidArgument, "unknown status %q", st)
	}
	rows, err := s.reader.ListSettlements(ctx, req.GetStatus())
	if err != nil {
		return nil, err
	}
	reply := &settlementv1.ListReply{}
	for _, row := range rows {
		reply.Settlements = append(reply.Settlements, toProto(row, nil))
	}
	return reply, nil
}

func (s *Server) Calculate(ctx context.Context, req *settlementv1.IDRequest) (*settlementv1.Settlement, error) {
	if err := s.cmds.Calculate(ctx, req.GetSettlementId()); err != nil {
		return nil, err
	}
	return s.load(ctx, req.GetSettlementId())
}

func (s *Server) SubmitReview(ctx context.Context, req *settlementv1.IDRequest) (*settlementv1.Settlement, error) {
	if err := s.cmds.SubmitReview(ctx, req.GetSettlementId()); err != nil {
		return nil, err
	}
	return s.load(ctx, req.GetSettlementId())
}

func (s *Server) Approve(ctx context.Context, req *settlementv1.IDRequest) (*settlementv1.Settlement, error) {
	if err := s.cmds.Approve(ctx, req.GetSettlementId()); err != nil {
		return nil, err
	}
	return s.load(ctx, req.GetSettlementId())
}

func (s *Server) Reject(ctx context.Context, req *settlementv1.RejectRequest) (*settlementv1.Settlement, error) {
	if req.GetReason() == "" {
		return nil, status.Error(codes.InvalidArgument, "a reject reason is required")
	}
	if err := s.cmds.Reject(ctx, req.GetSettlementId(), req.GetReason()); err != nil {
		return nil, err
	}
	return s.load(ctx, req.GetSettlementId())
}

func (s *Server) Pay(ctx context.Context, req *settlementv1.IDRequest) (*settlementv1.Settlement, error) {
	if err := s.cmds.Pay(ctx, req.GetSettlementId()); err != nil {
		return nil, err
	}
	return s.load(ctx, req.GetSettlementId())
}

// load reads a settlement with its items.
func (s *Server) load(ctx context.Context, id int64) (*settlementv1.Settlement, error) {
	row, err := s.reader.GetSettlement(ctx, id)
	if err != nil {
		return nil, err
	}
	items, err := s.reader.ListItems(ctx, id)
	if err != nil {
		return nil, err
	}
	return toProto(row, items), nil
}

func toProto(row domain.Settlement, items []store.Item) *settlementv1.Settlement {
	out := &settlementv1.Settlement{
		SettlementId: row.ID,
		Period:       row.Period,
		CustomerId:   row.CustomerID,
		Amount:       row.Amount.String(),
		Status:       string(row.Status),
		Version:      int64(row.Version),
	}
	for _, it := range items {
		pi := &settlementv1.SettlementItem{
			InvoiceId: it.InvoiceID,
			Amount:    it.Amount.String(),
			Verified:  it.Verified,
		}
		for _, c := range it.Checks {
			pi.Checks = append(pi.Checks, formatCheck(c.Name, c.Passed, c.Detail))
		}
		out.Items = append(out.Items, pi)
	}
	return out
}

func formatCheck(name string, passed bool, detail string) string {
	verdict := "passed"
	if !passed {
		verdict = "failed"
	}
	if detail == "" {
		return fmt.Sprintf("%s: %s", name, verdict)
	}
	return fmt.Sprintf("%s: %s (%s)", name, verdict, detail)
}
