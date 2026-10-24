//go:build lesson && integration

package callback

import (
	"context"
	"database/sql"
	"errors"
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

// handleRetrying delivers cb the way a supplier does: a busy order is sent again until the deadline.
func handleRetrying(ctx context.Context, h *Handler, cb Callback) (Result, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		res, err := h.Handle(ctx, cb)
		if !errors.Is(err, ErrBusy) || time.Now().After(deadline) {
			return res, err
		}
		time.Sleep(time.Duration(5+rand.IntN(20)) * time.Millisecond)
	}
}

// outboxKeys lists the routing keys of the events written for one order's task.
func outboxKeys(t *testing.T, db *sql.DB, o store.Order) []string {
	t.Helper()
	rows, err := db.Query(`SELECT routing_key FROM outbox WHERE JSON_EXTRACT(body, '$.payload.task_id') = ? ORDER BY id`, o.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	return keys
}

func reload(t *testing.T, db *sql.DB, o store.Order) store.Order {
	t.Helper()
	got, err := store.NewRepo(db).GetBySupplierOrder(context.Background(), db, o.Supplier, o.SupplierOrderNo)
	if err != nil {
		t.Fatal(err)
	}
	return got
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
		dups     int
	)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := handleRetrying(ctx, h, Callback{
				Supplier: order.Supplier, SupplierOrderNo: order.SupplierOrderNo, EventSeq: 1,
				Outcome: OutcomeSucceeded, InvoiceNo: "AIR-A-12345678",
				RawInserted: rand.IntN(2) == 0, // informational only
			})
			if err != nil {
				t.Errorf("worker %d: %v", i, err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			switch res {
			case Accepted:
				accepted++
			case Duplicate:
				dups++
			}
		}()
	}
	close(start)
	wg.Wait()

	if accepted != 1 || dups != workers-1 {
		t.Fatalf("accepted %d, duplicate %d, want 1 and %d", accepted, dups, workers-1)
	}
	if keys := outboxKeys(t, db, order); len(keys) != 1 || keys[0] != mq.ProviderTaskSucceeded {
		t.Fatalf("outbox = %v, want one %s", keys, mq.ProviderTaskSucceeded)
	}
	got := reload(t, db, order)
	if got.Status != store.StatusSucceeded || got.Version != order.Version+1 {
		t.Fatalf("order is %s at version %d, want succeeded at version %d", got.Status, got.Version, order.Version+1)
	}
}

func TestHandle_SucceededThenOlderFailed_Ignored(t *testing.T) {
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

	got := reload(t, db, order)
	if got.Status != store.StatusSucceeded || got.InvoiceNo != "AIR-A-87654321" {
		t.Fatalf("order is %s with invoice_no %q, want succeeded with AIR-A-87654321", got.Status, got.InvoiceNo)
	}
	if keys := outboxKeys(t, db, order); len(keys) != 1 || keys[0] != mq.ProviderTaskSucceeded {
		t.Fatalf("outbox = %v, want only %s", keys, mq.ProviderTaskSucceeded)
	}
}

func TestHandle_SucceededThenNewerFailed_Ignored(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "provider")
	h := NewHandler(db, store.NewRepo(db), lock.New(openRedis(t)))
	order := submittedOrder(t, db)

	res, err := h.Handle(ctx, Callback{
		Supplier: order.Supplier, SupplierOrderNo: order.SupplierOrderNo, EventSeq: 1,
		Outcome: OutcomeSucceeded, InvoiceNo: "AIR-A-97531246", RawInserted: true,
	})
	if err != nil || res != Accepted {
		t.Fatalf("seq 1 succeeded: %v %v, want Accepted", res, err)
	}
	res, err = h.Handle(ctx, Callback{
		Supplier: order.Supplier, SupplierOrderNo: order.SupplierOrderNo, EventSeq: 2,
		Outcome: OutcomeFailed, RawInserted: true,
	})
	if err != nil || res != Duplicate {
		t.Fatalf("newer seq 2 failed: %v %v, want Duplicate", res, err)
	}

	got := reload(t, db, order)
	if got.Status != store.StatusSucceeded || got.InvoiceNo != "AIR-A-97531246" {
		t.Fatalf("order is %s with invoice_no %q, want succeeded with AIR-A-97531246", got.Status, got.InvoiceNo)
	}
	if keys := outboxKeys(t, db, order); len(keys) != 1 || keys[0] != mq.ProviderTaskSucceeded {
		t.Fatalf("outbox = %v, want only %s", keys, mq.ProviderTaskSucceeded)
	}
}

func TestHandle_LockHeld_ReturnsBusyAndChangesNothing(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "provider")
	locks := lock.New(openRedis(t))
	h := NewHandler(db, store.NewRepo(db), locks)
	order := submittedOrder(t, db)

	key := lock.CallbackKey(order.Supplier, order.SupplierOrderNo)
	token, ok, err := locks.Acquire(ctx, key, lock.CallbackTTL)
	if err != nil || !ok {
		t.Fatalf("pre-acquire: ok=%v err=%v", ok, err)
	}
	t.Cleanup(func() { _ = locks.Release(context.Background(), key, token) })

	_, err = h.Handle(ctx, Callback{
		Supplier: order.Supplier, SupplierOrderNo: order.SupplierOrderNo, EventSeq: 1,
		Outcome: OutcomeSucceeded, InvoiceNo: "AIR-A-11223344", RawInserted: true,
	})
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("err = %v, want ErrBusy", err)
	}
	got := reload(t, db, order)
	if got.Status != order.Status || got.Version != order.Version || got.LastEventSeq != order.LastEventSeq {
		t.Fatalf("order changed under a held lock: %+v -> %+v", order, got)
	}
	if keys := outboxKeys(t, db, order); len(keys) != 0 {
		t.Fatalf("outbox = %v, want none", keys)
	}
}

func TestHandle_FailedThenNewerSucceeded_TwoEventsFinalSucceeded(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "provider")
	h := NewHandler(db, store.NewRepo(db), lock.New(openRedis(t)))
	order := submittedOrder(t, db)

	res, err := h.Handle(ctx, Callback{
		Supplier: order.Supplier, SupplierOrderNo: order.SupplierOrderNo, EventSeq: 1,
		Outcome: OutcomeFailed, RawInserted: true,
	})
	if err != nil || res != Accepted {
		t.Fatalf("seq 1 failed: %v %v, want Accepted", res, err)
	}
	res, err = h.Handle(ctx, Callback{
		Supplier: order.Supplier, SupplierOrderNo: order.SupplierOrderNo, EventSeq: 2,
		Outcome: OutcomeSucceeded, InvoiceNo: "AIR-A-24681357", RawInserted: true,
	})
	if err != nil || res != Accepted {
		t.Fatalf("seq 2 succeeded: %v %v, want Accepted", res, err)
	}

	got := reload(t, db, order)
	if got.Status != store.StatusSucceeded || got.InvoiceNo != "AIR-A-24681357" {
		t.Fatalf("order is %s with invoice_no %q, want succeeded with AIR-A-24681357", got.Status, got.InvoiceNo)
	}
	keys := outboxKeys(t, db, order)
	if len(keys) != 2 || keys[0] != mq.ProviderTaskFailed || keys[1] != mq.ProviderTaskSucceeded {
		t.Fatalf("outbox = %v, want failed then succeeded", keys)
	}
}

func TestHandle_TwoSeqsConcurrently_BothAppliedInOrder(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "provider")
	h := NewHandler(db, store.NewRepo(db), lock.New(openRedis(t)))
	order := submittedOrder(t, db)

	cbs := []Callback{
		{Supplier: order.Supplier, SupplierOrderNo: order.SupplierOrderNo, EventSeq: 1, Outcome: OutcomeFailed, RawInserted: true},
		{Supplier: order.Supplier, SupplierOrderNo: order.SupplierOrderNo, EventSeq: 2, Outcome: OutcomeSucceeded, InvoiceNo: "AIR-A-13572468", RawInserted: true},
	}
	var wg sync.WaitGroup
	for _, cb := range cbs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := handleRetrying(ctx, h, cb); err != nil {
				t.Errorf("seq %d: %v", cb.EventSeq, err)
			}
		}()
	}
	wg.Wait()

	got := reload(t, db, order)
	if got.Status != store.StatusSucceeded || got.LastEventSeq != 2 {
		t.Fatalf("order is %s at seq %d, want succeeded at seq 2", got.Status, got.LastEventSeq)
	}
	keys := outboxKeys(t, db, order)
	// Failed may be skipped when seq 2 wins the race, but must never follow succeeded.
	seenSucceeded := false
	for _, k := range keys {
		if k == mq.ProviderTaskSucceeded {
			seenSucceeded = true
		} else if seenSucceeded {
			t.Fatalf("outbox = %v: an event with a regressed seq follows succeeded", keys)
		}
	}
	if !seenSucceeded || len(keys) > 2 {
		t.Fatalf("outbox = %v, want succeeded, optionally preceded by failed", keys)
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
