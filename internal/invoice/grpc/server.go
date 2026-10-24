// Package invoicegrpc serves invoicehub.invoice.v1.InvoiceService.
package invoicegrpc

import (
	"context"
	"encoding/base64"
	"errors"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	invoicev1 "github.com/mafuky/invoice-hub/gen/invoicehub/invoice/v1"
	"github.com/mafuky/invoice-hub/internal/invoice/app"
	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/platform/grpcx"
	"github.com/mafuky/invoice-hub/internal/platform/money"
)

const (
	defaultPageSize = 50
	maxPageSize     = 200
)

// Commands is the write side of the invoice service.
type Commands interface {
	Apply(ctx context.Context, in app.ApplyInput) (invoiceID int64, existed bool, err error)
	RedFlush(ctx context.Context, invoiceID int64, reason string) (commandID int64, err error)
	Reissue(ctx context.Context, invoiceID int64, reason string) (commandID int64, err error)
	Supplement(ctx context.Context, in app.ApplyInput) (invoiceID, commandID int64, err error)
}

// Reader is the read side of the invoice service.
type Reader interface {
	GetInvoice(ctx context.Context, id int64) (domain.Invoice, error)
	ListInvoices(ctx context.Context, status string, afterID int64, limit int) ([]domain.Invoice, error)
}

type Server struct {
	invoicev1.UnimplementedInvoiceServiceServer
	cmds   Commands
	reader Reader
}

func NewServer(cmds Commands, reader Reader) *Server {
	return &Server{cmds: cmds, reader: reader}
}

// NewGRPCServer returns a grpc.Server with s registered behind the shared interceptors.
func NewGRPCServer(s *Server) *grpc.Server {
	gs := grpcx.NewServer("invoice", mapErr)
	invoicev1.RegisterInvoiceServiceServer(gs, s)
	return gs
}

func mapErr(_ context.Context, _ string, err error) (error, bool) {
	switch {
	case errors.Is(err, app.ErrBizOrderExists):
		return status.Error(codes.AlreadyExists, "biz_order_no already has an invoice"), true
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, "not found"), true
	}
	return nil, false
}

func (s *Server) Apply(ctx context.Context, req *invoicev1.ApplyRequest) (*invoicev1.ApplyReply, error) {
	in, err := toApplyInput(req.GetBizOrderNo(), req.GetProductLine(), req.GetTitle(), req.GetTaxNo(), req.GetAmount(), req.GetCustomerId())
	if err != nil {
		return nil, err
	}
	id, existed, err := s.cmds.Apply(ctx, in)
	if err != nil {
		return nil, err
	}
	return &invoicev1.ApplyReply{InvoiceId: id, Existed: existed}, nil
}

func (s *Server) Get(ctx context.Context, req *invoicev1.GetRequest) (*invoicev1.Invoice, error) {
	inv, err := s.reader.GetInvoice(ctx, req.GetInvoiceId())
	if err != nil {
		return nil, err
	}
	return toProto(inv), nil
}

func (s *Server) List(ctx context.Context, req *invoicev1.ListRequest) (*invoicev1.ListReply, error) {
	if st := req.GetStatus(); st != "" && !domain.KnownInvoiceStatus(domain.InvoiceStatus(st)) {
		return nil, status.Errorf(codes.InvalidArgument, "unknown status %q", st)
	}
	size := int(req.GetPageSize())
	switch {
	case size <= 0:
		size = defaultPageSize
	case size > maxPageSize:
		size = maxPageSize
	}
	after, err := decodeToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid page_token")
	}

	rows, err := s.reader.ListInvoices(ctx, req.GetStatus(), after, size+1)
	if err != nil {
		return nil, err
	}
	reply := &invoicev1.ListReply{}
	if len(rows) > size {
		rows = rows[:size]
		reply.NextPageToken = encodeToken(rows[len(rows)-1].ID)
	}
	for _, inv := range rows {
		reply.Invoices = append(reply.Invoices, toProto(inv))
	}
	return reply, nil
}

func (s *Server) RedFlush(ctx context.Context, req *invoicev1.RedFlushRequest) (*invoicev1.CommandReply, error) {
	id, err := s.cmds.RedFlush(ctx, req.GetInvoiceId(), req.GetReason())
	if err != nil {
		return nil, err
	}
	return commandReply(req.GetInvoiceId(), id), nil
}

func (s *Server) Reissue(ctx context.Context, req *invoicev1.ReissueRequest) (*invoicev1.CommandReply, error) {
	id, err := s.cmds.Reissue(ctx, req.GetInvoiceId(), req.GetReason())
	if err != nil {
		return nil, err
	}
	return commandReply(req.GetInvoiceId(), id), nil
}

func (s *Server) Supplement(ctx context.Context, req *invoicev1.SupplementRequest) (*invoicev1.CommandReply, error) {
	in, err := toApplyInput(req.GetBizOrderNo(), req.GetProductLine(), req.GetTitle(), req.GetTaxNo(), req.GetAmount(), req.GetCustomerId())
	if err != nil {
		return nil, err
	}
	invoiceID, commandID, err := s.cmds.Supplement(ctx, in)
	if err != nil {
		return nil, err
	}
	return commandReply(invoiceID, commandID), nil
}

func commandReply(invoiceID, commandID int64) *invoicev1.CommandReply {
	return &invoicev1.CommandReply{InvoiceId: invoiceID, Status: string(domain.CommandCreated), CommandId: commandID}
}

func toApplyInput(bizOrderNo string, line invoicev1.ProductLine, title, taxNo, amount, customerID string) (app.ApplyInput, error) {
	if bizOrderNo == "" || title == "" || taxNo == "" || customerID == "" {
		return app.ApplyInput{}, status.Error(codes.InvalidArgument, "biz_order_no, title, tax_no and customer_id are required")
	}
	pl, ok := productLineName(line)
	if !ok {
		return app.ApplyInput{}, status.Error(codes.InvalidArgument, "product_line must be FLIGHT, TRAIN or HOTEL")
	}
	amt, err := money.Parse(amount)
	if err != nil {
		return app.ApplyInput{}, status.Error(codes.InvalidArgument, "amount must be a decimal string such as \"100.10\"")
	}
	return app.ApplyInput{
		BizOrderNo: bizOrderNo, ProductLine: pl, Title: title, TaxNo: taxNo, Amount: amt, CustomerID: customerID,
	}, nil
}

func productLineName(l invoicev1.ProductLine) (string, bool) {
	switch l {
	case invoicev1.ProductLine_PRODUCT_LINE_FLIGHT:
		return "FLIGHT", true
	case invoicev1.ProductLine_PRODUCT_LINE_TRAIN:
		return "TRAIN", true
	case invoicev1.ProductLine_PRODUCT_LINE_HOTEL:
		return "HOTEL", true
	}
	return "", false
}

func productLineEnum(s string) invoicev1.ProductLine {
	switch s {
	case "FLIGHT":
		return invoicev1.ProductLine_PRODUCT_LINE_FLIGHT
	case "TRAIN":
		return invoicev1.ProductLine_PRODUCT_LINE_TRAIN
	case "HOTEL":
		return invoicev1.ProductLine_PRODUCT_LINE_HOTEL
	}
	return invoicev1.ProductLine_PRODUCT_LINE_UNSPECIFIED
}

func toProto(inv domain.Invoice) *invoicev1.Invoice {
	return &invoicev1.Invoice{
		InvoiceId:   inv.ID,
		BizOrderNo:  inv.BizOrderNo,
		ProductLine: productLineEnum(inv.ProductLine),
		Title:       inv.Title,
		TaxNo:       inv.TaxNo,
		Amount:      inv.Amount.String(),
		Status:      string(inv.Status),
		Version:     int64(inv.Version),
		CreatedAt:   timestamppb.New(inv.CreatedAt),
		UpdatedAt:   timestamppb.New(inv.UpdatedAt),
	}
}

func encodeToken(lastID int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(lastID, 10)))
}

func decodeToken(tok string) (int64, error) {
	if tok == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return 0, err
	}
	id, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || id < 0 {
		return 0, errors.New("bad token")
	}
	return id, nil
}
