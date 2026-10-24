//go:build lesson && integration

package callback

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mafuky/invoice-hub/internal/platform/config"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
	"github.com/mafuky/invoice-hub/internal/provider/lock"
	"github.com/mafuky/invoice-hub/internal/provider/store"
)

func runID() string { return fmt.Sprintf("%08x", rand.Uint32()) }

func openRedis(t *testing.T) *redis.Client {
	t.Helper()
	cfg, err := config.Load()
	if err != nil {
		t.Skipf("no config: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPassword})
	t.Cleanup(func() { rdb.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	return rdb
}

// submittedOrder seeds an order the supplier has accepted and is yet to call back about.
func submittedOrder(t *testing.T, db *sql.DB) store.Order {
	t.Helper()
	ctx := context.Background()
	repo := store.NewRepo(db)
	no := "AIR-A-ORD-" + runID()
	taskID := int64(rand.IntN(1_000_000) + 1)
	if _, err := repo.InsertOrder(ctx, db, store.NewOrder{TaskID: taskID, InvoiceID: taskID + 1, Supplier: "air-a", SupplierOrderNo: no}); err != nil {
		t.Fatal(err)
	}
	o, err := repo.GetBySupplierOrder(ctx, db, "air-a", no)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func countRows(t *testing.T, db *sql.DB, q string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestHandle_ConcurrentDuplicateCallbacks_OneOutboxEvent(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "provider")
	h := NewHandler(db, store.NewRepo(db), lock.New(openRedis(t)))
	order := submittedOrder(t, db)

	const workers = 10
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		accepted int
	)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := h.Handle(ctx, Callback{
				Supplier: order.Supplier, SupplierOrderNo: order.SupplierOrderNo, EventSeq: 1,
				Outcome: OutcomeSucceeded, InvoiceNo: "AIR-A-12345678",
				// Only one of the concurrent payload stores can add a new document.
				RawInserted: i == 0,
			})
			if err != nil {
				t.Logf("worker %d: %v", i, err)
				return
			}
			if res == Accepted {
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	if accepted != 1 {
		t.Fatalf("%d callbacks accepted, want 1", accepted)
	}
	if n := countRows(t, db, `SELECT COUNT(*) FROM outbox WHERE routing_key = ?`, mq.ProviderTaskSucceeded); n != 1 {
		t.Fatalf("succeeded events in outbox = %d, want 1", n)
	}
	got, err := store.NewRepo(db).GetBySupplierOrder(ctx, db, order.Supplier, order.SupplierOrderNo)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != store.StatusSucceeded || got.Version != order.Version+1 {
		t.Fatalf("order is %s at version %d, want succeeded at version %d", got.Status, got.Version, order.Version+1)
	}
}

func TestHandle_LateFailedAfterSucceeded_Ignored(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "provider")
	h := NewHandler(db, store.NewRepo(db), lock.New(openRedis(t)))
	order := submittedOrder(t, db)

	res, err := h.Handle(ctx, Callback{
		Supplier: order.Supplier, SupplierOrderNo: order.SupplierOrderNo, EventSeq: 2,
		Outcome: OutcomeSucceeded, InvoiceNo: "AIR-A-87654321", RawInserted: true,
	})
	if err != nil || res != Accepted {
		t.Fatalf("seq 2 succeeded: %v %v, want Accepted", res, err)
	}
	res, err = h.Handle(ctx, Callback{
		Supplier: order.Supplier, SupplierOrderNo: order.SupplierOrderNo, EventSeq: 1,
		Outcome: OutcomeFailed, RawInserted: true,
	})
	if err != nil || res != Duplicate {
		t.Fatalf("late seq 1 failed: %v %v, want Duplicate", res, err)
	}

	got, err := store.NewRepo(db).GetBySupplierOrder(ctx, db, order.Supplier, order.SupplierOrderNo)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != store.StatusSucceeded || got.InvoiceNo != "AIR-A-87654321" {
		t.Fatalf("order is %s with invoice_no %q, want succeeded with AIR-A-87654321", got.Status, got.InvoiceNo)
	}
	if n := countRows(t, db, `SELECT COUNT(*) FROM outbox WHERE routing_key = ?`, mq.ProviderTaskFailed); n != 0 {
		t.Fatalf("failed events in outbox = %d, want 0", n)
	}
}

func TestHandle_UnknownOrder_ReturnsUnknownOrder(t *testing.T) {
	db := testenv.OpenMySQL(t, "provider")
	h := NewHandler(db, store.NewRepo(db), lock.New(openRedis(t)))
	res, err := h.Handle(context.Background(), Callback{
		Supplier: "air-a", SupplierOrderNo: "AIR-A-ORD-" + runID(), EventSeq: 1,
		Outcome: OutcomeSucceeded, InvoiceNo: "AIR-A-11112222", RawInserted: true,
	})
	if err != nil || res != UnknownOrder {
		t.Fatalf("got %v %v, want UnknownOrder", res, err)
	}
}
