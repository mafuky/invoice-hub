package grpcx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/mafuky/invoice-hub/internal/platform/logx"
)

var errBusy = errors.New("busy")

func mapBusy(_ context.Context, _ string, err error) (error, bool) {
	if errors.Is(err, errBusy) {
		return status.Error(codes.Unavailable, "busy"), true
	}
	return nil, false
}

func call(t *testing.T, ctx context.Context, h grpc.UnaryHandler) error {
	t.Helper()
	info := &grpc.UnaryServerInfo{FullMethod: "/svc/Method"}
	chain := func(ctx context.Context, req any) (any, error) {
		return recoverPanic("test")(ctx, req, info, h)
	}
	_, err := traceInterceptor("test", mapBusy)(ctx, nil, info, func(ctx context.Context, req any) (any, error) { return chain(ctx, req) })
	return err
}

func TestInterceptors_ErrorMapping(t *testing.T) {
	cases := []struct {
		name string
		err  error
		code codes.Code
		msg  string
	}{
		{"unfinished lesson", fmt.Errorf("lesson 5: %w", errors.ErrUnsupported), codes.Unimplemented, "not implemented yet (lesson 5)"},
		{"service error", fmt.Errorf("lock: %w", errBusy), codes.Unavailable, ""},
		{"canceled", context.Canceled, codes.Canceled, ""},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded, ""},
		{"unknown", errors.New("db down"), codes.Internal, "internal error"},
		{"existing status", status.Error(codes.NotFound, "x"), codes.NotFound, "x"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := call(t, context.Background(), func(context.Context, any) (any, error) { return nil, c.err })
			st, _ := status.FromError(err)
			if st.Code() != c.code || (c.msg != "" && !strings.Contains(st.Message(), c.msg)) {
				t.Fatalf("got %v %q, want %v %q", st.Code(), st.Message(), c.code, c.msg)
			}
		})
	}
}

func TestInterceptors_TraceHeaderReachesHandler(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(TraceHeader, "trace-0001"))
	var seen string
	_ = call(t, ctx, func(ctx context.Context, _ any) (any, error) { seen = logx.TraceID(ctx); return nil, nil })
	if seen != "trace-0001" {
		t.Fatalf("handler saw trace %q", seen)
	}
}

func TestInterceptors_PanicBecomesInternal(t *testing.T) {
	err := call(t, context.Background(), func(context.Context, any) (any, error) { panic("boom") })
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v, want Internal", status.Code(err))
	}
}
