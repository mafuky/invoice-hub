// Package api exposes invoice and settlement operations over HTTP.
package api

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	invoicev1 "github.com/mafuky/invoice-hub/gen/invoicehub/invoice/v1"
	providerv1 "github.com/mafuky/invoice-hub/gen/invoicehub/provider/v1"
	settlementv1 "github.com/mafuky/invoice-hub/gen/invoicehub/settlement/v1"
	"github.com/mafuky/invoice-hub/internal/platform/grpcx"
	"github.com/mafuky/invoice-hub/internal/platform/logx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type CallbackClient interface {
	HandleCallback(context.Context, *providerv1.CallbackRequest, ...grpc.CallOption) (*providerv1.CallbackReply, error)
}

type handlers struct {
	invoices    invoicev1.InvoiceServiceClient
	provider    CallbackClient
	settlements settlementv1.SettlementServiceClient
}

// NewRouter accepts generated client interfaces so handlers can be tested without sockets.
func NewRouter(invoices invoicev1.InvoiceServiceClient, provider CallbackClient, settlements settlementv1.SettlementServiceClient) *gin.Engine {
	h := handlers{invoices, provider, settlements}
	r := gin.New()
	r.HandleMethodNotAllowed = true
	r.Use(func(c *gin.Context) {
		trace := c.GetHeader("X-Trace-Id")
		if trace == "" {
			trace = uuid.NewString()
		}
		c.Header("X-Trace-Id", trace)
		ctx := logx.WithTrace(c.Request.Context(), trace)
		ctx = metadata.AppendToOutgoingContext(ctx, grpcx.TraceHeader, trace)
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
		c.Next()
	}, gin.CustomRecoveryWithWriter(io.Discard, func(c *gin.Context, _ any) { fail(c, 500, "internal error") }))
	r.NoRoute(func(c *gin.Context) { fail(c, 404, "not found") })
	r.NoMethod(func(c *gin.Context) { fail(c, 405, "method not allowed") })
	r.GET("/healthz", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })
	r.POST("/invoices", h.apply)
	r.GET("/invoices", h.list)
	r.POST("/invoices/supplement", h.supplement)
	r.GET("/invoices/:id", h.get)
	r.POST("/invoices/:id/red-flush", h.redFlush)
	r.POST("/invoices/:id/reissue", h.reissue)
	r.POST("/callbacks/:supplier", h.callback)
	r.GET("/settlements/:id", h.getSettlement)
	for _, action := range []string{"calculate", "submit", "approve", "reject", "pay"} {
		r.POST("/settlements/:id/"+action, h.settlementAction(action))
	}
	return r
}

func fail(c *gin.Context, code int, message string) {
	c.AbortWithStatusJSON(code, gin.H{"error": message})
}
func rpcError(c *gin.Context, err error) {
	code := 500
	switch status.Code(err) {
	case codes.Unimplemented:
		code = 501
	case codes.NotFound:
		code = 404
	case codes.InvalidArgument:
		code = 400
	case codes.FailedPrecondition, codes.AlreadyExists:
		code = 409
	case codes.Unavailable:
		code = 503
	case codes.DeadlineExceeded:
		code = 504
	}
	message := "internal error"
	if code != 500 {
		message = status.Convert(err).Message()
	}
	fail(c, code, message)
}
