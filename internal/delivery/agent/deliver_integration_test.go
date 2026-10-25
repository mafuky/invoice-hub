//go:build lesson && integration

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/mafuky/invoice-hub/internal/delivery/channel"
	"github.com/mafuky/invoice-hub/internal/delivery/store"
	"github.com/mafuky/invoice-hub/internal/delivery/store/storetest"
	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
)

const arrivalTimeout = 60 * time.Second

func invoiceIssuedEnvelope(t *testing.T, invoiceID int64, version int) event.Envelope {
	t.Helper()
	amt, err := money.Parse("1999.99")
	if err != nil {
		t.Fatal(err)
	}
	env, err := event.New(mq.InvoiceIssued, strconv.FormatInt(invoiceID, 10), version, event.InvoiceIssued{
		InvoiceID: invoiceID, Version: version, InvoiceNo: "AIR-A-12345678", CustomerID: "CUST-0001",
		ProductLine: "FLIGHT", Title: "Example Travel Co., Ltd.", Amount: amt,
	}, "trace-issued")
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func TestOnInvoiceIssued_TwiceCreatesTwoDeliveriesOnly(t *testing.T) {
	ctx := context.Background()
	repo := storetest.NewRepo(t)
	a := NewDeliveryAgent(repo, nil)

	env := invoiceIssuedEnvelope(t, 7101, 1)
	for i := 0; i < 2; i++ {
		if err := a.OnInvoiceIssued(ctx, env); err != nil {
			t.Fatalf("delivery %d of the event: %v", i+1, err)
		}
	}

	if n, err := repo.Deliveries().CountDocuments(ctx, bson.D{{Key: "invoice_id", Value: int64(7101)}}); err != nil || n != 2 {
		t.Fatalf("deliveries = %d (err %v), want 2", n, err)
	}
	for _, ch := range store.Channels {
		d, err := repo.Find(ctx, 7101, 1, ch)
		if err != nil {
			t.Fatalf("channel %s: %v", ch, err)
		}
		if d.Status != store.StatusPending {
			t.Fatalf("channel %s status = %s, want pending", ch, d.Status)
		}
	}
}

// receiver stands in for the customer endpoint. The first push is recorded and
// then answered too late; later pushes are answered at once.
type receiver struct {
	mu     sync.Mutex
	pushes []map[string]any
}

func (r *receiver) handler(hold time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var b map[string]any
		if err := json.NewDecoder(req.Body).Decode(&b); err != nil {
			http.Error(w, "invalid invoice push", http.StatusBadRequest)
			return
		}
		r.mu.Lock()
		r.pushes = append(r.pushes, b)
		first := len(r.pushes) == 1
		r.mu.Unlock()
		if first {
			select {
			case <-time.After(hold):
			case <-req.Context().Done():
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	})
}

func TestDeliver_SlowSuccessIsPushedAgainWithSameIdentity(t *testing.T) {
	ctx := context.Background()
	repo := storetest.NewRepo(t)

	rcv := &receiver{}
	srv := httptest.NewServer(rcv.handler(2 * time.Second))
	t.Cleanup(srv.Close)
	a := NewDeliveryAgent(repo, map[string]Sender{
		store.ChannelCustomer: channel.NewCustomer(srv.URL, 200*time.Millisecond),
	})

	if err := repo.EnsureInvoiceSummary(ctx, 7102, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.InsertPending(ctx, store.NewDelivery{
		InvoiceID: 7102, InvoiceVersion: 1, Channel: store.ChannelCustomer,
		CustomerID: "CUST-0001", InvoiceNo: "AIR-A-12345678", Amount: "1999.99",
	}); err != nil {
		t.Fatal(err)
	}
	d, err := repo.Find(ctx, 7102, 1, store.ChannelCustomer)
	if err != nil {
		t.Fatal(err)
	}

	if err := a.Deliver(ctx, d); err == nil {
		t.Fatal("first Deliver succeeded although the customer endpoint answered too late")
	}

	d, err = repo.Find(ctx, 7102, 1, store.ChannelCustomer)
	if err != nil {
		t.Fatal(err)
	}
	if d.Status != store.StatusPending || d.Attempt != 1 {
		t.Fatalf("after timeout = %+v, want pending with attempt 1", d)
	}
	a.now = func() time.Time { return d.NextRetryAt.Add(time.Millisecond) }
	if err := a.Deliver(ctx, d); err != nil {
		t.Fatalf("second Deliver: %v", err)
	}

	// At least once: the customer saw the invoice twice, both times with the
	// identity it needs to drop the repeat.
	rcv.mu.Lock()
	defer rcv.mu.Unlock()
	if len(rcv.pushes) != 2 {
		t.Fatalf("customer received %d pushes, want 2", len(rcv.pushes))
	}
	for i, p := range rcv.pushes {
		if p["invoice_id"] != "7102" || p["invoice_version"] != float64(1) {
			t.Fatalf("push %d = %v, want invoice_id 7102 and invoice_version 1", i+1, p)
		}
	}
	if n, err := repo.Deliveries().CountDocuments(ctx, bson.D{{Key: "invoice_id", Value: int64(7102)}}); err != nil || n != 1 {
		t.Fatalf("delivery records = %d (err %v), want 1", n, err)
	}
	got, err := repo.Find(ctx, 7102, 1, store.ChannelCustomer)
	if err != nil || got.Status != store.StatusDelivered {
		t.Fatalf("final record = %+v (err %v), want delivered", got, err)
	}
	assertAnnouncementRows(t, repo, store.DeliveredKey(7102, 1), mq.DeliverySucceeded, 0)
}

func TestDeliver_DeliveredAnnouncedOnceAfterBothChannels(t *testing.T) {
	ctx := context.Background()
	repo := storetest.NewRepo(t)
	a := agentWithHTTP(t, repo, http.StatusOK)
	createDeliveries(t, repo, 7103)
	for i, ch := range []string{store.ChannelCustomer, store.ChannelFinance} {
		d := findDelivery(t, repo, 7103, ch)
		if ch == store.ChannelFinance {
			if claimed, _, err := repo.MarkDelivering(ctx, d.ID, time.Now().UTC()); err != nil || !claimed {
				t.Fatalf("finance claim = %v, %v", claimed, err)
			}
			d = findDelivery(t, repo, 7103, ch)
		}
		if err := a.Deliver(ctx, d); err != nil {
			t.Fatalf("deliver to %s: %v", ch, err)
		}
		if got := findDelivery(t, repo, 7103, ch); got.Status != store.StatusDelivered {
			t.Fatalf("%s record = %+v, want delivered", ch, got)
		}
		assertAnnouncementRows(t, repo, store.DeliveredKey(7103, 1), mq.DeliverySucceeded, int64(i))
	}
	for _, ch := range store.Channels {
		if err := a.Deliver(ctx, findDelivery(t, repo, 7103, ch)); err != nil {
			t.Fatalf("repeat delivered %s: %v", ch, err)
		}
	}
	assertAnnouncementRows(t, repo, store.DeliveredKey(7103, 1), mq.DeliverySucceeded, 1)
}

func TestDeliver_GaveUpAnnouncedOnce(t *testing.T) {
	ctx := context.Background()
	repo := storetest.NewRepo(t)
	a := agentWithHTTP(t, repo, http.StatusInternalServerError)
	createDeliveries(t, repo, 7104)
	for _, ch := range []string{store.ChannelCustomer, store.ChannelFinance} {
		d := findDelivery(t, repo, 7104, ch)
		if _, err := repo.Deliveries().UpdateByID(ctx, d.ID, bson.D{{Key: "$set", Value: bson.D{
			{Key: "attempt", Value: MaxAttempts - 1},
		}}}); err != nil {
			t.Fatal(err)
		}
		d = findDelivery(t, repo, 7104, ch)
		if err := a.Deliver(ctx, d); err != nil && !errors.Is(err, channel.ErrPush) {
			t.Fatalf("deliver to %s: unexpected error %v", ch, err)
		}
		got := findDelivery(t, repo, 7104, ch)
		if got.Status != store.StatusGaveUp || got.Attempt != MaxAttempts {
			t.Fatalf("%s record = %+v, want gave_up with attempt %d", ch, got, MaxAttempts)
		}
		assertAnnouncementRows(t, repo, store.GaveUpKey(7104, 1), mq.DeliveryGaveUp, 1)
	}
}

func TestDeliver_LostClaimLeavesRecordAlone(t *testing.T) {
	for _, tc := range []struct {
		name            string
		status, attempt int
	}{
		{"success", http.StatusOK, 0},
		{"retry", http.StatusInternalServerError, 0},
		{"gave_up", http.StatusInternalServerError, MaxAttempts - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			repo := storetest.NewRepo(t)
			a := agentWithHTTP(t, repo, tc.status)
			createDeliveries(t, repo, 7105)
			// Finance has finished; an unfenced customer success would announce delivery.
			finance := NewDeliveryAgent(repo, a.senders)
			if tc.status != http.StatusOK {
				finance = agentWithHTTP(t, repo, http.StatusOK)
			}
			if err := finance.Deliver(ctx, findDelivery(t, repo, 7105, store.ChannelFinance)); err != nil {
				t.Fatal(err)
			}
			d := findDelivery(t, repo, 7105, store.ChannelCustomer)
			if _, err := repo.Deliveries().UpdateByID(ctx, d.ID, bson.D{{Key: "$set", Value: bson.D{
				{Key: "attempt", Value: tc.attempt},
			}}}); err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC().Truncate(time.Millisecond)
			claimed, oldSince, err := repo.MarkDelivering(ctx, d.ID, now.Add(-10*time.Minute))
			if err != nil || !claimed {
				t.Fatalf("first claim = %v, %v", claimed, err)
			}
			old := findDelivery(t, repo, 7105, store.ChannelCustomer)
			if ok, err := repo.Requeue(ctx, d.ID, now.Add(-time.Minute), now); err != nil || !ok {
				t.Fatalf("stale requeue = %v, %v", ok, err)
			}
			claimed, newSince, err := repo.MarkDelivering(ctx, d.ID, now)
			if err != nil || !claimed || newSince.Equal(oldSince) {
				t.Fatalf("second claim = %v, %v, %v", claimed, newSince, err)
			}
			before := findDelivery(t, repo, 7105, store.ChannelCustomer)
			assertAnnouncementRows(t, repo, store.DeliveredKey(7105, 1), mq.DeliverySucceeded, 0)
			if err := a.Deliver(ctx, old); err != nil {
				t.Fatalf("lost claim must return nil, got %v", err)
			}
			after := findDelivery(t, repo, 7105, store.ChannelCustomer)
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("lost claim changed record: before %+v, after %+v", before, after)
			}
			assertAnnouncementRows(t, repo, store.DeliveredKey(7105, 1), mq.DeliverySucceeded, 0)
		})
	}
}

type senderFunc func(context.Context, store.Delivery) error

func (f senderFunc) Send(ctx context.Context, d store.Delivery) error { return f(ctx, d) }

func TestDeliver_ClaimLostDuringPush_LeavesRecordAlone(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pushErr error
		attempt int
	}{
		{"success", nil, 0},
		{"retry", channel.ErrPush, 0},
		{"gave_up", channel.ErrPush, MaxAttempts - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := storetest.NewRepo(t)
			ctx, cancel := context.WithTimeout(context.Background(), arrivalTimeout)
			defer cancel()
			createDeliveries(t, repo, 7106)
			// Finance has finished, so an unfenced customer success would announce delivery.
			finance := agentWithHTTP(t, repo, http.StatusOK)
			if err := finance.Deliver(ctx, findDelivery(t, repo, 7106, store.ChannelFinance)); err != nil {
				t.Fatal(err)
			}
			d := findDelivery(t, repo, 7106, store.ChannelCustomer)
			if _, err := repo.Deliveries().UpdateByID(ctx, d.ID, bson.D{{Key: "$set", Value: bson.D{
				{Key: "attempt", Value: tc.attempt},
			}}}); err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC().Truncate(time.Millisecond)
			claimed, oldSince, err := repo.MarkDelivering(ctx, d.ID, now.Add(-10*time.Minute))
			if err != nil || !claimed {
				t.Fatalf("first claim = %v, %v", claimed, err)
			}
			old := findDelivery(t, repo, 7106, store.ChannelCustomer)
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			a := NewDeliveryAgent(repo, map[string]Sender{
				store.ChannelCustomer: senderFunc(func(ctx context.Context, _ store.Delivery) error {
					select {
					case entered <- struct{}{}:
					case <-ctx.Done():
						return ctx.Err()
					}
					select {
					case <-release:
						return tc.pushErr
					case <-ctx.Done():
						return ctx.Err()
					}
				}),
			})
			result := make(chan error, 1)
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				result <- a.Deliver(ctx, old)
			}()
			defer func() {
				cancel()
				unblock()
				<-finished
			}()
			select {
			case <-entered:
			case err := <-result:
				t.Fatalf("Deliver returned before the push: %v", err)
			case <-ctx.Done():
				t.Fatalf("waiting for push: %v", ctx.Err())
			}

			// Replace the claim only while Send is blocked, before its outcome is known.
			if ok, err := repo.Requeue(ctx, d.ID, now.Add(-time.Minute), now); err != nil || !ok {
				t.Fatalf("stale requeue = %v, %v", ok, err)
			}
			claimed, newSince, err := repo.MarkDelivering(ctx, d.ID, now)
			if err != nil || !claimed || newSince.Equal(oldSince) {
				t.Fatalf("second claim = %v, %v, %v", claimed, newSince, err)
			}
			before := findDelivery(t, repo, 7106, store.ChannelCustomer)
			if before.Status != store.StatusDelivering || before.DeliveringSince == nil ||
				!before.DeliveringSince.Equal(newSince) || before.Attempt != tc.attempt {
				t.Fatalf("second claim = %+v, want delivering at %v with attempt %d", before, newSince, tc.attempt)
			}
			assertAnnouncementRows(t, repo, store.DeliveredKey(7106, 1), mq.DeliverySucceeded, 0)
			unblock()
			select {
			case err := <-result:
				if err != nil {
					t.Errorf("lost claim must return nil, got %v", err)
				}
			case <-ctx.Done():
				t.Fatalf("waiting for Deliver: %v", ctx.Err())
			}
			after := findDelivery(t, repo, 7106, store.ChannelCustomer)
			if !reflect.DeepEqual(before, after) {
				t.Errorf("lost claim changed record: before %+v, after %+v", before, after)
			}
			assertAnnouncementRows(t, repo, store.DeliveredKey(7106, 1), mq.DeliverySucceeded, 0)
		})
	}
}

func agentWithHTTP(t *testing.T, repo *store.Repo, status int) *DeliveryAgent {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || (r.URL.Path != "/customers/CUST-0001/push" && r.URL.Path != "/finance/writeback") {
			t.Errorf("unexpected invoice push: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return NewDeliveryAgent(repo, map[string]Sender{
		store.ChannelCustomer: channel.NewCustomer(srv.URL, time.Second),
		store.ChannelFinance:  channel.NewFinance(srv.URL, time.Second),
	})
}

func createDeliveries(t *testing.T, repo *store.Repo, invoiceID int64) {
	t.Helper()
	ctx := context.Background()
	if err := repo.EnsureInvoiceSummary(ctx, invoiceID, 1); err != nil {
		t.Fatal(err)
	}
	for _, ch := range store.Channels {
		if _, err := repo.InsertPending(ctx, store.NewDelivery{
			InvoiceID: invoiceID, InvoiceVersion: 1, Channel: ch,
			CustomerID: "CUST-0001", InvoiceNo: "AIR-A-12345678", Amount: "1999.99",
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func findDelivery(t *testing.T, repo *store.Repo, invoiceID int64, ch string) store.Delivery {
	t.Helper()
	d, err := repo.Find(context.Background(), invoiceID, 1, ch)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func assertAnnouncementRows(t *testing.T, repo *store.Repo, key, routingKey string, want int64) {
	t.Helper()
	ctx := context.Background()
	for name, filter := range map[string]bson.D{
		"dedupe key": {{Key: "dedupe_key", Value: key}},
		"event":      {{Key: "dedupe_key", Value: key}, {Key: "routing_key", Value: routingKey}},
		"all outbox": {},
	} {
		if n, err := repo.OutboxCollection().CountDocuments(ctx, filter); err != nil || n != want {
			t.Fatalf("%s rows = %d (err %v), want %d", name, n, err, want)
		}
	}
}
