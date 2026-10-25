package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	invoicev1 "github.com/mafuky/invoice-hub/gen/invoicehub/invoice/v1"
	providerv1 "github.com/mafuky/invoice-hub/gen/invoicehub/provider/v1"
	settlementv1 "github.com/mafuky/invoice-hub/gen/invoicehub/settlement/v1"
	"github.com/mafuky/invoice-hub/internal/api"
	"github.com/mafuky/invoice-hub/internal/platform/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func target(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err == nil && (host == "" || host == "0.0.0.0" || host == "::") {
		return net.JoinHostPort("127.0.0.1", port)
	}
	return addr
}
func run() error {
	cfg, err := config.Load()
	if err != nil {
		return errors.New("api: configuration could not be loaded; check INVHUB settings")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var conns []*grpc.ClientConn
	defer func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}()
	for _, addr := range []string{cfg.InvoiceAddr, cfg.ProviderAddr, cfg.SettlementAddr} {
		conn, err := grpc.NewClient(target(addr), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return errors.New("api: could not create service client")
		}
		conns = append(conns, conn)
	}
	gin.SetMode(gin.ReleaseMode)
	server := &http.Server{Addr: cfg.APIAddr, Handler: api.NewRouter(invoicev1.NewInvoiceServiceClient(conns[0]), providerv1.NewProviderServiceClient(conns[1]), settlementv1.NewSettlementServiceClient(conns[2])), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- server.ListenAndServe() }()
	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return errors.New("api: HTTP listener failed")
		}
		return nil
	case <-ctx.Done():
		grace, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(grace); err != nil {
			_ = server.Close()
			return errors.New("api: graceful shutdown timed out")
		}
		return nil
	}
}
