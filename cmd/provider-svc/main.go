// Command provider-svc places supplier orders and applies supplier callbacks.
//
// Subcommands: serve (gRPC plus the consume and relay loops), relay-only and
// consume-only.
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
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	"github.com/mafuky/invoice-hub/internal/platform/config"
	"github.com/mafuky/invoice-hub/internal/platform/dial"
	"github.com/mafuky/invoice-hub/internal/platform/logx"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
	"github.com/mafuky/invoice-hub/internal/platform/outbox"
	platformstore "github.com/mafuky/invoice-hub/internal/platform/store"
	"github.com/mafuky/invoice-hub/internal/provider/agent"
	"github.com/mafuky/invoice-hub/internal/provider/callback"
	"github.com/mafuky/invoice-hub/internal/provider/client"
	providergrpc "github.com/mafuky/invoice-hub/internal/provider/grpc"
	"github.com/mafuky/invoice-hub/internal/provider/lock"
	"github.com/mafuky/invoice-hub/internal/provider/store"
)

const (
	consumePrefetch = 16
	shutdownGrace   = 10 * time.Second
)

func main() {
	if len(os.Args) != 2 || (os.Args[1] != "serve" && os.Args[1] != "relay-only" && os.Args[1] != "consume-only") {
		fmt.Fprintln(os.Stderr, "usage: provider-svc serve|relay-only|consume-only")
		os.Exit(2)
	}
	log := logx.New("provider-svc")
	slog.SetDefault(log)
	if err := run(os.Args[1], log); err != nil {
		log.Error("provider-svc stopped", "err", err)
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
	db, err := platformstore.OpenMySQL(ctx, cfg.MySQLDSNProvider)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := platformstore.MigrateMySQL(ctx, cfg.MySQLDSNProvider, "provider"); err != nil {
		return err
	}

	mongoDB, err := platformstore.OpenMongo(ctx, cfg.MongoURI, cfg.MongoDB)
	if err != nil {
		return err
	}
	defer func() { _ = mongoDB.Client().Disconnect(context.Background()) }()
	if err := platformstore.EnsureMongoIndexes(ctx, mongoDB); err != nil {
		return err
	}

	rdb, err := openRedis(ctx, cfg.RedisAddr, cfg.RedisPassword)
	if err != nil {
		return err
	}
	defer rdb.Close()

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
	mock := client.NewMock(cfg.MockURL)
	provider := agent.NewProviderAgent(db, repo, mock, agent.DefaultSuppliers())

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	loops := loopGroup{errc: make(chan error, 4)}

	var gs *grpc.Server
	if mode == "serve" {
		lis, err := net.Listen("tcp", cfg.ProviderAddr)
		if err != nil {
			return fmt.Errorf("listen %s: %w", cfg.ProviderAddr, err)
		}
		handler := callback.NewHandler(db, repo, lock.New(rdb))
		raw := store.NewRawCallbacks(mongoDB.Collection("callbacks"))
		gs = providergrpc.NewGRPCServer(providergrpc.NewServer(handler, raw, repo, mock))
		log.Info("listening " + cfg.ProviderAddr)
		loops.start(runCtx, "grpc", func() error { return gs.Serve(lis) })
	}
	if mode == "serve" || mode == "consume-only" {
		loops.start(runCtx, "consume", func() error {
			return mq.Consume(runCtx, conn, mq.QueueProvider, consumePrefetch, provider.OnTaskCreated)
		})
	}
	if mode == "serve" || mode == "relay-only" {
		pub, err := mq.NewPublisher(conn)
		if err != nil {
			return err
		}
		defer pub.Close()
		relay := outbox.NewRelay(db, pub, "provider")
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

// openRedis checks the server is reachable first so a closed tunnel fails fast with a hint.
func openRedis(ctx context.Context, addr, password string) (*redis.Client, error) {
	if err := dial.Check(addr, 5*time.Second, "run `make tunnel`"); err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, Password: password})
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis: ping %s failed", addr)
	}
	return rdb, nil
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
