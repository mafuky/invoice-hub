// Command mock-supplier serves the fake suppliers, customer endpoint,
// finance center and verification source described by a scenario file.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mafuky/invoice-hub/internal/mock"
	"github.com/mafuky/invoice-hub/internal/platform/config"
	"github.com/mafuky/invoice-hub/internal/platform/logx"
)

func main() {
	scenarioPath := flag.String("scenario", "scenarios/happy.yaml", "path to the scenario file")
	flag.Parse()

	log := logx.New("mock-supplier")
	if err := run(log, *scenarioPath); err != nil {
		log.Error("mock-supplier stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, scenarioPath string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	sc, err := mock.LoadScenario(scenarioPath)
	if err != nil {
		return err
	}

	m := mock.NewServer(sc, cfg.MockCallbackURL)
	srv := &http.Server{Addr: cfg.MockAddr, Handler: m, ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		m.Close()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()

	log.Info("mock-supplier listening", "addr", cfg.MockAddr, "scenario", scenarioPath, "callback_url", cfg.MockCallbackURL)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
