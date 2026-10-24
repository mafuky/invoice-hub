// Command delivery-svc delivers issued invoices to customers and the finance center.
//
// Subcommand: serve (consume, delivery worker, stuck-delivery scanner, outbox
// relay and the health endpoint).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/mafuky/invoice-hub/internal/delivery/agent"
	"github.com/mafuky/invoice-hub/internal/delivery/channel"
	deliveryoutbox "github.com/mafuky/invoice-hub/internal/delivery/outbox"
	"github.com/mafuky/invoice-hub/internal/delivery/scan"
	"github.com/mafuky/invoice-hub/internal/delivery/store"
	"github.com/mafuky/invoice-hub/internal/platform/config"
	"github.com/mafuky/invoice-hub/internal/platform/dial"
	"github.com/mafuky/invoice-hub/internal/platform/logx"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
	platformstore "github.com/mafuky/invoice-hub/internal/platform/store"
)

const (
	consumePrefetch = 16
	workerInterval  = time.Second
	scanInterval    = 30 * time.Second
	shutdownGrace   = 10 * time.Second
)

func main() {
	if len(os.Args) != 2 || os.Args[1] != "serve" {
		fmt.Fprintln(os.Stderr, "usage: delivery-svc serve")
		os.Exit(2)
	}
	log := logx.New("delivery-svc")
	slog.SetDefault(log)
	if err := run(log); err != nil {
		log.Error("delivery-svc stopped", "err", err)
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
	mongoDB, err := platformstore.OpenMongo(ctx, cfg.MongoURI, cfg.MongoDB)
	if err != nil {
		return err
	}
	defer func() { _ = mongoDB.Client().Disconnect(context.Background()) }()
	if err := store.RequireTransactions(ctx, mongoDB); err != nil {
		return err
	}
	if err := platformstore.EnsureMongoIndexes(ctx, mongoDB); err != nil {
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

	lis, err := net.Listen("tcp", cfg.DeliveryAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.DeliveryAddr, err)
	}
	pub, err := mq.NewPublisher(conn)
	if err != nil {
		_ = lis.Close()
		return err
	}
	defer pub.Close()

	repo := store.NewRepo(mongoDB)
	deliveries := agent.NewDeliveryAgent(repo, map[string]agent.Sender{
		store.ChannelCustomer: channel.NewCustomer(cfg.MockURL, cfg.DeliveryTimeout),
		store.ChannelFinance:  channel.NewFinance(cfg.MockURL, cfg.DeliveryTimeout),
	})
	scanner := scan.NewScanner(repo)
	relay := deliveryoutbox.NewRelay(repo.OutboxCollection(), pub, "delivery")

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	loops := loopGroup{errc: make(chan error, 5)}

	srv := &http.Server{Handler: healthMux(), ReadHeaderTimeout: 5 * time.Second}
	log.Info("listening " + cfg.DeliveryAddr)
	loops.start("http", func() error {
		if err := srv.Serve(lis); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	loops.start("consume", func() error {
		return mq.Consume(runCtx, conn, mq.QueueDelivery, consumePrefetch, deliveries.OnInvoiceIssued)
	})
	loops.start("worker", func() error { return deliveries.Run(runCtx, workerInterval) })
	loops.start("scan", func() error { return scanner.Run(runCtx, scanInterval, cfg.DeliveryStuckAfter) })
	loops.start("relay", func() error { return relay.Run(runCtx) })

	var runErr error
	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case runErr = <-loops.errc:
		log.Error("background loop failed, shutting down", "err", runErr)
	}

	cancel()
	grace, graceCancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer graceCancel()
	if err := srv.Shutdown(grace); err != nil {
		_ = srv.Close()
	}
	if !loops.wait(grace) {
		log.Warn("loops did not stop within the grace period")
	}
	return runErr
}

func healthMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	return mux
}

// loopGroup runs background loops and reports the first one that fails.
// A loop that returns nil before shutdown counts as a failure too.
type loopGroup struct {
	wg   sync.WaitGroup
	errc chan error
}

func (g *loopGroup) start(name string, fn func() error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		err := fn()
		if err == nil {
			err = errors.New("stopped unexpectedly")
		}
		select {
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
