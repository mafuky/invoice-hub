// Command invoice-svc owns invoices, their commands and tasks.
//
// Subcommands: serve (gRPC plus the consume, relay and timeout loops),
// relay-only and consume-only.
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
	"strings"
	"sync"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	providerv1 "github.com/mafuky/invoice-hub/gen/invoicehub/provider/v1"
	"github.com/mafuky/invoice-hub/internal/invoice/app"
	invoicegrpc "github.com/mafuky/invoice-hub/internal/invoice/grpc"
	"github.com/mafuky/invoice-hub/internal/invoice/store"
	"github.com/mafuky/invoice-hub/internal/invoice/store/gormstore"
	"github.com/mafuky/invoice-hub/internal/platform/config"
	"github.com/mafuky/invoice-hub/internal/platform/consume"
	"github.com/mafuky/invoice-hub/internal/platform/dial"
	"github.com/mafuky/invoice-hub/internal/platform/logx"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
	"github.com/mafuky/invoice-hub/internal/platform/outbox"
	platformstore "github.com/mafuky/invoice-hub/internal/platform/store"
)

const (
	consumePrefetch = 16
	scanInterval    = 5 * time.Second
	shutdownGrace   = 10 * time.Second
)

func main() {
	if len(os.Args) != 2 || (os.Args[1] != "serve" && os.Args[1] != "relay-only" && os.Args[1] != "consume-only") {
		fmt.Fprintln(os.Stderr, "usage: invoice-svc serve|relay-only|consume-only")
		os.Exit(2)
	}
	log := logx.New("invoice-svc")
	slog.SetDefault(log)
	if err := run(os.Args[1], log); err != nil {
		log.Error("invoice-svc stopped", "err", err)
		os.Exit(1)
	}
}

func run(mode string, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	db, err := platformstore.OpenMySQL(ctx, cfg.MySQLDSNInvoice)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := platformstore.MigrateMySQL(ctx, cfg.MySQLDSNInvoice, "invoice"); err != nil {
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

	var repo store.Store = store.NewRepo(db)
	if cfg.StoreImpl == "gorm" {
		repo, err = gormstore.NewRepo(db)
		if err != nil {
			return err
		}
	}
	persistence := app.WithPersistence(consume.ForImplementation(cfg.StoreImpl), outbox.ForImplementation(cfg.StoreImpl))
	events := app.NewEvents(db, repo, cfg.CallbackDeadline, persistence)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	loops := loopGroup{errc: make(chan error, 4)}

	var gs *grpc.Server
	if mode == "serve" {
		providerConn, err := grpc.NewClient(dialTarget(cfg.ProviderAddr), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("provider client: %w", err)
		}
		defer providerConn.Close()

		lis, err := net.Listen("tcp", cfg.InvoiceAddr)
		if err != nil {
			return fmt.Errorf("listen %s: %w", cfg.InvoiceAddr, err)
		}
		gs = invoicegrpc.NewGRPCServer(invoicegrpc.NewServer(app.NewService(db, repo, persistence), repo))
		log.Info("listening " + cfg.InvoiceAddr)
		loops.start(runCtx, "grpc", func() error { return gs.Serve(lis) })

		scanner := app.NewScanner(db, repo, providerv1.NewProviderServiceClient(providerConn), events)
		loops.start(runCtx, "timeout scanner", func() error { return scanner.Run(runCtx, scanInterval) })
	}
	if mode == "serve" || mode == "consume-only" {
		loops.start(runCtx, "consume", func() error {
			return mq.Consume(runCtx, conn, mq.QueueInvoice, consumePrefetch, events.Handle)
		})
	}
	if mode == "serve" || mode == "relay-only" {
		pub, err := mq.NewPublisher(conn)
		if err != nil {
			return err
		}
		defer pub.Close()
		relay := outbox.NewRelayForImplementation(db, pub, "invoice", cfg.StoreImpl)
		loops.start(runCtx, "relay", func() error { return relay.Run(runCtx) })
	}

	runErr := waitForLoop(ctx, loops.errc)
	if runErr != nil {
		log.Error("background loop failed, shutting down", "err", runErr)
	} else {
		log.Info("shutting down")
	}

	cancel()
	grace, graceCancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer graceCancel()
	if gs != nil {
		stopped := make(chan struct{})
		go func() { gs.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-grace.Done():
			gs.Stop()
		}
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

// dialTarget turns ":9102" into an address gRPC can dial.
func dialTarget(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "127.0.0.1" + addr
	}
	return addr
}
