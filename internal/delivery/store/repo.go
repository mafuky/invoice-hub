// Package store persists delivery records, the outbox and processed events in MongoDB.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/mafuky/invoice-hub/internal/delivery/outbox"
	"github.com/mafuky/invoice-hub/internal/platform/event"
)

// deliveries.status values.
const (
	StatusPending    = "pending"
	StatusDelivering = "delivering"
	StatusDelivered  = "delivered"
	StatusGaveUp     = "gave_up"
)

// Delivery channels.
const (
	ChannelCustomer = "customer"
	ChannelFinance  = "finance"
)

// Channels lists every channel an issued invoice must reach.
var Channels = []string{ChannelCustomer, ChannelFinance}

var ErrNotFound = errors.New("delivery not found")

// ErrClaimLost means the delivery is no longer held by the caller: it was
// taken back and possibly claimed by another worker since the caller claimed it.
var ErrClaimLost = errors.New("delivery claim lost")

// Outcome is how a channel ended for an invoice version.
type Outcome string

const (
	OutcomeDelivered Outcome = "delivered"
	OutcomeGaveUp    Outcome = "gave_up"
)

// DeliveredKey and GaveUpKey are the outbox dedupe keys of the two announcements.
func DeliveredKey(invoiceID int64, version int) string {
	return fmt.Sprintf("delivered:%d:%d", invoiceID, version)
}

func GaveUpKey(invoiceID int64, version int) string {
	return fmt.Sprintf("gave_up:%d:%d", invoiceID, version)
}

// Delivery is one invoice version on its way to one channel.
type Delivery struct {
	ID             bson.ObjectID `bson:"_id"`
	InvoiceID      int64         `bson:"invoice_id"`
	InvoiceVersion int           `bson:"invoice_version"`
	Channel        string        `bson:"channel"`
	CustomerID     string        `bson:"customer_id"`
	InvoiceNo      string        `bson:"invoice_no"`
	Amount         string        `bson:"amount"`
	Status         string        `bson:"status"`
	Attempt        int           `bson:"attempt"`
	NextRetryAt    time.Time     `bson:"next_retry_at"`
	// DeliveringSince is set while Status is delivering.
	DeliveringSince *time.Time `bson:"delivering_since,omitempty"`
	DeliveredAt     *time.Time `bson:"delivered_at,omitempty"`
	LastError       string     `bson:"last_error,omitempty"`
	CreatedAt       time.Time  `bson:"created_at"`
	UpdatedAt       time.Time  `bson:"updated_at"`
}

// NewDelivery is a delivery that has not been recorded yet.
type NewDelivery struct {
	InvoiceID      int64
	InvoiceVersion int
	Channel        string
	CustomerID     string
	InvoiceNo      string
	// Amount is a two-decimal string such as "1999.99".
	Amount string
}

// Repo reads and writes delivery-svc's collections. Methods that take a
// context join the transaction that context belongs to, if any.
type Repo struct {
	db         *mongo.Database
	deliveries *mongo.Collection
	outbox     *mongo.Collection
	processed  *mongo.Collection
	invoices   *mongo.Collection
}

// NewRepo uses the deliveries, outbox and processed_events collections of db.
func NewRepo(db *mongo.Database) *Repo { return NewRepoWithPrefix(db, "") }

// NewRepoWithPrefix prefixes the collection names, so tests can work in collections of their own.
func NewRepoWithPrefix(db *mongo.Database, prefix string) *Repo {
	return &Repo{
		db:         db,
		deliveries: db.Collection(prefix + "deliveries"),
		outbox:     db.Collection(prefix + "outbox"),
		processed:  db.Collection(prefix + "processed_events"),
		invoices:   db.Collection(prefix + "delivery_invoices"),
	}
}

// Deliveries is the deliveries collection, for the integration tests.
func (r *Repo) Deliveries() *mongo.Collection { return r.deliveries }

// OutboxCollection is the collection the outbox relay reads.
func (r *Repo) OutboxCollection() *mongo.Collection { return r.outbox }

// ProcessedEvents is the processed_events collection.
func (r *Repo) ProcessedEvents() *mongo.Collection { return r.processed }

// WithTx runs fn in one MongoDB transaction. Repo calls made with the context
// fn receives commit or roll back together; an error from fn rolls everything back.
// Transactions need a replica set. fn may run more than once (transient errors
// and write conflicts are retried); do only database work in it, never send pushes.
func (r *Repo) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	sess, err := r.db.Client().StartSession()
	if err != nil {
		return fmt.Errorf("store: start session: %w", err)
	}
	defer sess.EndSession(context.WithoutCancel(ctx))
	_, err = sess.WithTransaction(ctx, func(sc context.Context) (any, error) {
		return nil, fn(sc)
	})
	return err
}

// InsertPending records a pending delivery due now and reports inserted=false
// when the same invoice version is already recorded for the channel.
func (r *Repo) InsertPending(ctx context.Context, nd NewDelivery) (inserted bool, err error) {
	now := time.Now().UTC()
	_, err = r.deliveries.InsertOne(ctx, Delivery{
		ID:             bson.NewObjectID(),
		InvoiceID:      nd.InvoiceID,
		InvoiceVersion: nd.InvoiceVersion,
		Channel:        nd.Channel,
		CustomerID:     nd.CustomerID,
		InvoiceNo:      nd.InvoiceNo,
		Amount:         nd.Amount,
		Status:         StatusPending,
		NextRetryAt:    now,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	if mongo.IsDuplicateKeyError(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: insert delivery of invoice %d v%d to %s: %w", nd.InvoiceID, nd.InvoiceVersion, nd.Channel, err)
	}
	return true, nil
}

// Get returns one delivery by id.
func (r *Repo) Get(ctx context.Context, id bson.ObjectID) (Delivery, error) {
	var d Delivery
	err := r.deliveries.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return Delivery{}, ErrNotFound
	}
	if err != nil {
		return Delivery{}, fmt.Errorf("store: get delivery %s: %w", id.Hex(), err)
	}
	return d, nil
}

// Find returns the delivery of one invoice version on one channel.
func (r *Repo) Find(ctx context.Context, invoiceID int64, version int, channel string) (Delivery, error) {
	var d Delivery
	err := r.deliveries.FindOne(ctx, bson.D{
		{Key: "invoice_id", Value: invoiceID},
		{Key: "invoice_version", Value: version},
		{Key: "channel", Value: channel},
	}).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return Delivery{}, ErrNotFound
	}
	if err != nil {
		return Delivery{}, fmt.Errorf("store: find delivery of invoice %d v%d to %s: %w", invoiceID, version, channel, err)
	}
	return d, nil
}

// ListDue returns up to limit pending deliveries whose retry time has come, oldest first.
func (r *Repo) ListDue(ctx context.Context, now time.Time, limit int) ([]Delivery, error) {
	return r.list(ctx, bson.D{
		{Key: "status", Value: StatusPending},
		{Key: "next_retry_at", Value: bson.D{{Key: "$lte", Value: now}}},
	}, bson.D{{Key: "next_retry_at", Value: 1}}, limit)
}

// ListStuck returns up to limit deliveries that have been delivering since before the cutoff.
func (r *Repo) ListStuck(ctx context.Context, before time.Time, limit int) ([]Delivery, error) {
	return r.list(ctx, bson.D{
		{Key: "status", Value: StatusDelivering},
		{Key: "delivering_since", Value: bson.D{{Key: "$lt", Value: before}}},
	}, bson.D{{Key: "delivering_since", Value: 1}}, limit)
}

func (r *Repo) list(ctx context.Context, filter, sort bson.D, limit int) ([]Delivery, error) {
	cur, err := r.deliveries.Find(ctx, filter, options.Find().SetSort(sort).SetLimit(int64(limit)))
	if err != nil {
		return nil, fmt.Errorf("store: list deliveries: %w", err)
	}
	var out []Delivery
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("store: read deliveries: %w", err)
	}
	return out, nil
}

// MarkDelivering moves a pending delivery to delivering and reports claimed=false
// when another worker got it first. since is the claim's token: the later
// transitions only apply while the record still carries it.
func (r *Repo) MarkDelivering(ctx context.Context, id bson.ObjectID, now time.Time) (claimed bool, since time.Time, err error) {
	since = now.UTC().Truncate(time.Millisecond)
	claimed, err = r.update(ctx, bson.D{{Key: "_id", Value: id}, {Key: "status", Value: StatusPending}}, bson.D{
		{Key: "status", Value: StatusDelivering},
		{Key: "delivering_since", Value: since},
	})
	return claimed, since, err
}

// MarkDelivered moves a delivery claimed at since to delivered, or returns ErrClaimLost.
func (r *Repo) MarkDelivered(ctx context.Context, id bson.ObjectID, since, now time.Time) error {
	return r.fenced(ctx, id, since, bson.D{
		{Key: "status", Value: StatusDelivered},
		{Key: "delivered_at", Value: now},
		{Key: "delivering_since", Value: nil},
	})
}

// MarkRetry puts a delivery claimed at since back to pending, due at next, with
// the attempt count so far, or returns ErrClaimLost.
func (r *Repo) MarkRetry(ctx context.Context, id bson.ObjectID, since time.Time, attempt int, next time.Time, lastErr string) error {
	return r.fenced(ctx, id, since, bson.D{
		{Key: "status", Value: StatusPending},
		{Key: "attempt", Value: attempt},
		{Key: "next_retry_at", Value: next},
		{Key: "last_error", Value: lastErr},
		{Key: "delivering_since", Value: nil},
	})
}

// MarkGaveUp ends a delivery claimed at since without success, or returns ErrClaimLost.
func (r *Repo) MarkGaveUp(ctx context.Context, id bson.ObjectID, since time.Time, attempt int, lastErr string) error {
	return r.fenced(ctx, id, since, bson.D{
		{Key: "status", Value: StatusGaveUp},
		{Key: "attempt", Value: attempt},
		{Key: "last_error", Value: lastErr},
		{Key: "delivering_since", Value: nil},
	})
}

func (r *Repo) fenced(ctx context.Context, id bson.ObjectID, since time.Time, set bson.D) error {
	ok, err := r.update(ctx, bson.D{
		{Key: "_id", Value: id},
		{Key: "status", Value: StatusDelivering},
		{Key: "delivering_since", Value: since.UTC().Truncate(time.Millisecond)},
	}, set)
	if err != nil {
		return err
	}
	if !ok {
		return ErrClaimLost
	}
	return nil
}

// Requeue puts a delivery back to pending, due now, only if it is still
// delivering and has been since before the cutoff.
func (r *Repo) Requeue(ctx context.Context, id bson.ObjectID, before, now time.Time) (requeued bool, err error) {
	return r.update(ctx, bson.D{
		{Key: "_id", Value: id},
		{Key: "status", Value: StatusDelivering},
		{Key: "delivering_since", Value: bson.D{{Key: "$lt", Value: before}}},
	}, bson.D{
		{Key: "status", Value: StatusPending},
		{Key: "next_retry_at", Value: now},
		{Key: "delivering_since", Value: nil},
	})
}

func (r *Repo) update(ctx context.Context, filter, set bson.D) (bool, error) {
	set = append(set, bson.E{Key: "updated_at", Value: time.Now().UTC()})
	res, err := r.deliveries.UpdateOne(ctx, filter, bson.D{{Key: "$set", Value: set}})
	if err != nil {
		return false, fmt.Errorf("store: update delivery: %w", err)
	}
	return res.ModifiedCount == 1, nil
}

// CountByStatus counts the deliveries of one invoice version that are in status.
func (r *Repo) CountByStatus(ctx context.Context, invoiceID int64, version int, status string) (int64, error) {
	n, err := r.deliveries.CountDocuments(ctx, bson.D{
		{Key: "invoice_id", Value: invoiceID},
		{Key: "invoice_version", Value: version},
		{Key: "status", Value: status},
	})
	if err != nil {
		return 0, fmt.Errorf("store: count %s deliveries of invoice %d v%d: %w", status, invoiceID, version, err)
	}
	return n, nil
}

// InsertOutbox queues env for the relay. Inside WithTx it commits with the
// delivery rows. A non-empty dedupeKey makes a second row with the same key fail.
func (r *Repo) InsertOutbox(ctx context.Context, routingKey string, env event.Envelope, dedupeKey string) error {
	return outbox.Insert(ctx, r.outbox, routingKey, env, dedupeKey)
}

// RecordChannelOutcome notes that a channel of an invoice version ended and
// reports which announcements the caller must now write to the outbox.
// It must run inside the delivery transaction, and it goes through one summary
// document per invoice version because counting the separate channel documents
// under snapshot isolation can miss the last channel; the shared document makes
// concurrent transactions conflict, and the loser is retried.
func (r *Repo) RecordChannelOutcome(sessCtx context.Context, invoiceID int64, version int, channel string, outcome Outcome) (announceDelivered, announceGaveUp bool, err error) {
	id := bson.D{{Key: "invoice_id", Value: invoiceID}, {Key: "version", Value: version}}
	field := "delivered"
	if outcome == OutcomeGaveUp {
		field = "gave_up"
	}
	_, err = r.invoices.UpdateOne(sessCtx, bson.D{{Key: "_id", Value: id}}, bson.D{
		{Key: "$addToSet", Value: bson.D{{Key: field, Value: channel}}},
		{Key: "$setOnInsert", Value: bson.D{
			{Key: "delivered_announced", Value: false},
			{Key: "gave_up_announced", Value: false},
		}},
	}, options.UpdateOne().SetUpsert(true))
	if err != nil {
		return false, false, fmt.Errorf("store: record %s of invoice %d v%d on %s: %w", outcome, invoiceID, version, channel, err)
	}

	switch outcome {
	case OutcomeDelivered:
		res, err := r.invoices.UpdateOne(sessCtx, bson.D{
			{Key: "_id", Value: id},
			{Key: "delivered_announced", Value: false},
			{Key: "delivered", Value: bson.D{{Key: "$all", Value: Channels}}},
		}, bson.D{{Key: "$set", Value: bson.D{{Key: "delivered_announced", Value: true}}}})
		if err != nil {
			return false, false, fmt.Errorf("store: announce delivered of invoice %d v%d: %w", invoiceID, version, err)
		}
		return res.ModifiedCount == 1, false, nil
	case OutcomeGaveUp:
		res, err := r.invoices.UpdateOne(sessCtx, bson.D{
			{Key: "_id", Value: id},
			{Key: "gave_up_announced", Value: false},
		}, bson.D{{Key: "$set", Value: bson.D{{Key: "gave_up_announced", Value: true}}}})
		if err != nil {
			return false, false, fmt.Errorf("store: announce gave up of invoice %d v%d: %w", invoiceID, version, err)
		}
		return false, res.ModifiedCount == 1, nil
	}
	return false, false, fmt.Errorf("store: unknown outcome %q", outcome)
}

// RequireTransactions fails unless db runs on a replica set or a sharded
// cluster, the only deployments where MongoDB transactions work.
func RequireTransactions(ctx context.Context, db *mongo.Database) error {
	var hello struct {
		SetName string `bson:"setName"`
		Msg     string `bson:"msg"`
	}
	if err := db.RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		return fmt.Errorf("store: cannot query mongo topology: %w", err)
	}
	if hello.SetName == "" && hello.Msg != "isdbgrid" {
		return errors.New("store: mongo is not a replica set; delivery-svc needs transactions, so start mongod with --replSet and run rs.initiate()")
	}
	return nil
}

// MarkProcessed records an event id and reports first=false when it was seen before.
func (r *Repo) MarkProcessed(ctx context.Context, eventID string) (first bool, err error) {
	_, err = r.processed.InsertOne(ctx, bson.D{
		{Key: "event_id", Value: eventID},
		{Key: "consumer", Value: "delivery-svc"},
		{Key: "processed_at", Value: time.Now().UTC()},
	})
	if mongo.IsDuplicateKeyError(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: mark event %s processed: %w", eventID, err)
	}
	return true, nil
}
