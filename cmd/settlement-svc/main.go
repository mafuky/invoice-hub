// Command settlement-svc owns the monthly settlements: it adds delivered
// invoices to them, verifies each against the supplier and runs the review and
// payment steps.
//
// Subcommand: serve (gRPC plus the consume and relay loops).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"

	"github.com/mafuky/invoice-hub/internal/platform/config"
	"github.com/mafuky/invoice-hub/internal/platform/dial"
	"github.com/mafuky/invoice-hub/internal/platform/logx"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
	"github.com/mafuky/invoice-hub/internal/platform/outbox"
	platformstore "github.com/mafuky/invoice-hub/internal/platform/store"
	"github.com/mafuky/invoice-hub/internal/settlement/app"
	settlementgrpc "github.com/mafuky/invoice-hub/internal/settlement/grpc"
	"github.com/mafuky/invoice-hub/internal/settlement/store"
	"github.com/mafuky/invoice-hub/internal/settlement/verify"
)

const (
	consumePrefetch = 16
	shutdownGrace   = 10 * time.Second
)

func main() {
	if len(os.Args) != 2 || os.Args[1] != "serve" {
		fmt.Fprintln(os.Stderr, "usage: settlement-svc serve")
		os.Exit(2)
	}
	log := logx.New("settlement-svc")
	slog.SetDefault(log)
	if err := run(log); err != nil {
		log.Error("settlement-svc stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	db, err := platformstore.OpenMySQL(ctx, cfg.MySQLDSNSettlement)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := platformstore.MigrateMySQL(ctx, cfg.MySQLDSNSettlement, "settlement"); err != nil {
		return err
	}

	conn, err := dialAMQP(cfg.AMQPURL)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := declareTopology(conn); err != nil {
		return err
	}
	log.Info("queues declared")

	repo := store.NewRepo(db)
	svc := app.NewService(db, repo, verify.NewClient(cfg.MockURL, cfg.DeliveryTimeout))

	lis, err := net.Listen("tcp", cfg.SettlementAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.SettlementAddr, err)
	}
	pub, err := mq.NewPublisher(conn)
	if err != nil {
		_ = lis.Close()
		return err
	}
	defer pub.Close()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	loops := loopGroup{errc: make(chan error, 3)}

	gs := settlementgrpc.NewGRPCServer(settlementgrpc.NewServer(svc, repo))
	log.Info("listening " + cfg.SettlementAddr)
	loops.start(runCtx, "grpc", func() error { return gs.Serve(lis) })
	loops.start(runCtx, "consume", func() error {
		return mq.Consume(runCtx, conn, mq.QueueSettlement, consumePrefetch, svc.OnInvoiceDelivered)
	})
	relay := outbox.NewRelay(db, pub, "settlement")
	loops.start(runCtx, "relay", func() error { return relay.Run(runCtx) })

	runErr := waitForLoop(ctx, loops.errc)
	if runErr != nil {
		log.Error("background loop failed, shutting down", "err", runErr)
	} else {
		log.Info("shutting down")
	}

	cancel()
	grace, graceCancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer graceCancel()
	stopped := make(chan struct{})
	go func() { gs.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-grace.Done():
		gs.Stop()
	}
	if !loops.wait(grace) {
		log.Warn("loops did not stop within the grace period")
	}
	return runErr
}

func waitForLoop(ctx context.Context, errc <-chan error) error {
	select {
	case <-ctx.Done():
		return nil
	default:
	}
	select {
	case <-ctx.Done():
		return nil
	case err := <-errc:
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
}

// loopGroup runs background loops and reports the first one that fails.
// A loop that returns nil before shutdown counts as a failure too.
type loopGroup struct {
	wg   sync.WaitGroup
	errc chan error
}

func (g *loopGroup) start(ctx context.Context, name string, fn func() error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		err := fn()
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			err = errors.New("stopped unexpectedly")
		}
		if errors.Is(err, grpc.ErrServerStopped) {
			return
		}
		select {
		case <-ctx.Done():
		case g.errc <- fmt.Errorf("%s: %w", name, err):
		default:
		}
	}()
}

func (g *loopGroup) wait(ctx context.Context) bool {
	done := make(chan struct{})
	go func() { g.wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

// dialAMQP checks the broker is reachable first so a closed tunnel fails fast
// with a hint. Errors never include the URL, which carries the password.
func dialAMQP(raw string) (*amqp.Connection, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return nil, errors.New("amqp: invalid INVHUB_AMQP_URL")
	}
	if err := dial.Check(u.Host, 5*time.Second, "run `make tunnel`"); err != nil {
		return nil, fmt.Errorf("amqp: %w", err)
	}
	conn, err := amqp.Dial(raw)
	if err != nil {
		return nil, errors.New("amqp: connect to " + u.Host + " failed")
	}
	return conn, nil
}

func declareTopology(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("amqp: open channel: %w", err)
	}
	defer ch.Close()
	return mq.Declare(ch)
}
