// Package grpcx holds the gRPC server plumbing the services share: trace
// propagation, panic recovery and mapping of errors to status codes.
package grpcx

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

	"github.com/mafuky/invoice-hub/internal/platform/logx"
)

// TraceHeader carries the caller's trace id.
const TraceHeader = "x-trace-id"

// MapErr turns a service-specific error into a status. ok=false hands the
// error to the shared mapping.
type MapErr func(ctx context.Context, method string, err error) (mapped error, ok bool)

var lessonRe = regexp.MustCompile(`lesson (\d+)`)

// NewServer returns a grpc.Server that puts the trace id into the context,
// recovers panics and maps errors to status codes.
func NewServer(service string, mapErr MapErr) *grpc.Server {
	return grpc.NewServer(grpc.ChainUnaryInterceptor(traceInterceptor(service, mapErr), recoverPanic(service)))
}

func traceInterceptor(service string, mapErr MapErr) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if v := md.Get(TraceHeader); len(v) > 0 && v[0] != "" {
				ctx = logx.WithTrace(ctx, v[0])
			}
		}
		resp, err := h(ctx, req)
		if err == nil {
			return resp, nil
		}
		return nil, toStatus(ctx, service, info.FullMethod, err, mapErr)
	}
}

func recoverPanic(service string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, service+" rpc panicked", "method", info.FullMethod, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
				resp, err = nil, status.Error(codes.Internal, "internal error")
			}
		}()
		return h(ctx, req)
	}
}

func toStatus(ctx context.Context, service, method string, err error, mapErr MapErr) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, errors.ErrUnsupported) {
		msg := "not implemented yet"
		if m := lessonRe.FindStringSubmatch(err.Error()); m != nil {
			msg += " (lesson " + m[1] + ")"
		}
		return status.Error(codes.Unimplemented, msg)
	}
	if mapErr != nil {
		if mapped, ok := mapErr(ctx, method, err); ok {
			return mapped
		}
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	}
	slog.ErrorContext(ctx, service+" rpc failed", "method", method, "err", err)
	return status.Error(codes.Internal, "internal error")
}
