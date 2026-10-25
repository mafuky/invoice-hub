//go:build integration

package lock

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mafuky/invoice-hub/internal/platform/config"
)

const arrivalTimeout = 60 * time.Second

func openRedis(t *testing.T) *redis.Client {
	t.Helper()
	cfg, err := config.Load()
	if err != nil {
		t.Skipf("no config: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPassword})
	t.Cleanup(func() { rdb.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), arrivalTimeout)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	return rdb
}

func TestLock_SecondHolderWaitsForRelease(t *testing.T) {
	ctx := context.Background()
	l := New(openRedis(t))
	key := CallbackKey("air-a", fmt.Sprintf("AIR-A-ORD-%08x", rand.Uint32()))

	tok, ok, err := l.Acquire(ctx, key, CallbackTTL)
	if err != nil || !ok {
		t.Fatalf("first acquire: ok=%v err=%v", ok, err)
	}
	if _, ok, _ := l.Acquire(ctx, key, CallbackTTL); ok {
		t.Fatal("second acquire succeeded while the lock was held")
	}
	if err := l.Release(ctx, key, "not-the-token"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := l.Acquire(ctx, key, CallbackTTL); ok {
		t.Fatal("release with a foreign token freed the lock")
	}
	if err := l.Release(ctx, key, tok); err != nil {
		t.Fatal(err)
	}
	tok2, ok, err := l.Acquire(ctx, key, CallbackTTL)
	if err != nil || !ok {
		t.Fatalf("acquire after release: ok=%v err=%v", ok, err)
	}
	_ = l.Release(ctx, key, tok2)
}

func TestLock_ExpiresAfterTTL(t *testing.T) {
	ctx := context.Background()
	l := New(openRedis(t))
	key := CallbackKey("air-a", fmt.Sprintf("AIR-A-ORD-%08x", rand.Uint32()))

	if _, ok, err := l.Acquire(ctx, key, 200*time.Millisecond); err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	time.Sleep(400 * time.Millisecond)
	tok, ok, err := l.Acquire(ctx, key, CallbackTTL)
	if err != nil || !ok {
		t.Fatalf("acquire after expiry: ok=%v err=%v", ok, err)
	}
	_ = l.Release(ctx, key, tok)
}
