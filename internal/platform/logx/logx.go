// Package logx builds slog loggers that carry the service name and trace id.
package logx

import (
	"context"
	"log/slog"
	"os"
)

type ctxKey struct{}

type traceHandler struct {
	slog.Handler
}

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := TraceID(ctx); id != "" {
		r.AddAttrs(slog.String("trace_id", id))
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return traceHandler{h.Handler.WithAttrs(a)}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{h.Handler.WithGroup(name)}
}

// New returns a JSON logger; use the *Context methods so trace_id is attached.
func New(service string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, nil)
	return slog.New(traceHandler{h}).With("service", service)
}

func WithTrace(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, ctxKey{}, traceID)
}

func TraceID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}
