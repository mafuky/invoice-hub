package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLoopGroup_CancellationIsNormal(t *testing.T) {
	for _, loopErr := range []error{nil, context.Canceled, errors.New("connection closed")} {
		t.Run(fmtError(loopErr), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			g := loopGroup{errc: make(chan error, 1)}
			g.start(ctx, "consume", func() error { <-ctx.Done(); return loopErr })
			cancel()
			waitCtx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if !g.wait(waitCtx) {
				t.Fatal("loop did not stop")
			}
			select {
			case err := <-g.errc:
				t.Fatalf("normal shutdown reported a failure: %v", err)
			default:
			}
		})
	}
}

func TestLoopGroup_PrematureReturnIsFailure(t *testing.T) {
	for _, loopErr := range []error{nil, errors.New("broker disconnected")} {
		t.Run(fmtError(loopErr), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			g := loopGroup{errc: make(chan error, 1)}
			g.start(ctx, "consume", func() error { return loopErr })
			err := waitForLoop(ctx, g.errc)
			if err == nil {
				t.Fatal("premature loop return was not reported")
			}
			if loopErr != nil && !errors.Is(err, loopErr) {
				t.Fatalf("loop error = %v, want %v", err, loopErr)
			}
			if !g.wait(ctx) {
				t.Fatal("loop did not finish")
			}
		})
	}
}

func TestWaitForLoop_PrefersShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 100 {
		errc := make(chan error, 1)
		errc <- errors.New("loop stopped")
		if err := waitForLoop(ctx, errc); err != nil {
			t.Fatalf("cancelled shutdown = %v", err)
		}
	}
}

func fmtError(err error) string {
	if err == nil {
		return "nil"
	}
	return err.Error()
}
