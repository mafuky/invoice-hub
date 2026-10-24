// Package outbox keeps delivery-svc events in MongoDB next to the delivery
// records and relays them to the broker.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/mafuky/invoice-hub/internal/platform/event"
)

// outbox.status values.
const (
	statusPending = "pending"
	statusSent    = "sent"
)

// row is one outbox document.
type row struct {
	ID          bson.ObjectID `bson:"_id"`
	EventID     string        `bson:"event_id"`
	RoutingKey  string        `bson:"routing_key"`
	Body        string        `bson:"body"`
	Status      string        `bson:"status"`
	Attempts    int           `bson:"attempts"`
	NextAttempt time.Time     `bson:"next_attempt_at"`
	LeaseUntil  time.Time     `bson:"lease_until"`
	LastError   string        `bson:"last_error,omitempty"`
	CreatedAt   time.Time     `bson:"created_at"`
	SentAt      *time.Time    `bson:"sent_at,omitempty"`
}

// Insert writes env into coll. Called with a transaction context it commits or
// rolls back together with the delivery records that caused the event.
func Insert(ctx context.Context, coll *mongo.Collection, routingKey string, env event.Envelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("outbox: marshal event %s: %w", env.EventID, err)
	}
	now := time.Now().UTC()
	_, err = coll.InsertOne(ctx, row{
		ID:          bson.NewObjectID(),
		EventID:     env.EventID,
		RoutingKey:  routingKey,
		Body:        string(body),
		Status:      statusPending,
		NextAttempt: now,
		LeaseUntil:  now,
		CreatedAt:   now,
	})
	if err != nil {
		return fmt.Errorf("outbox: insert event %s (%s): %w", env.EventID, routingKey, err)
	}
	return nil
}

// rows is what the relay needs from the outbox collection.
type rows interface {
	// claim leases one due row so no other relay sends it meanwhile.
	claim(ctx context.Context, now time.Time, lease time.Duration) (row, bool, error)
	markSent(ctx context.Context, id bson.ObjectID, at time.Time) error
	markFailed(ctx context.Context, id bson.ObjectID, attempts int, next time.Time, msg string) error
	// release gives the lease back without counting an attempt.
	release(ctx context.Context, id bson.ObjectID, at time.Time) error
}

type mongoRows struct{ coll *mongo.Collection }

func (m mongoRows) claim(ctx context.Context, now time.Time, lease time.Duration) (row, bool, error) {
	filter := bson.D{
		{Key: "status", Value: statusPending},
		{Key: "next_attempt_at", Value: bson.D{{Key: "$lte", Value: now}}},
		{Key: "lease_until", Value: bson.D{{Key: "$lte", Value: now}}},
	}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "lease_until", Value: now.Add(lease)}}}}
	opts := options.FindOneAndUpdate().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetReturnDocument(options.After)
	var r row
	err := m.coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&r)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return row{}, false, nil
	}
	if err != nil {
		return row{}, false, fmt.Errorf("outbox: claim: %w", err)
	}
	return r, true, nil
}

func (m mongoRows) markSent(ctx context.Context, id bson.ObjectID, at time.Time) error {
	_, err := m.coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, bson.D{{Key: "$set", Value: bson.D{
		{Key: "status", Value: statusSent},
		{Key: "sent_at", Value: at},
	}}})
	if err != nil {
		return fmt.Errorf("outbox: mark %s sent: %w", id.Hex(), err)
	}
	return nil
}

func (m mongoRows) markFailed(ctx context.Context, id bson.ObjectID, attempts int, next time.Time, msg string) error {
	_, err := m.coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, bson.D{{Key: "$set", Value: bson.D{
		{Key: "attempts", Value: attempts},
		{Key: "next_attempt_at", Value: next},
		{Key: "last_error", Value: msg},
	}}})
	if err != nil {
		return fmt.Errorf("outbox: mark %s failed: %w", id.Hex(), err)
	}
	return nil
}

func (m mongoRows) release(ctx context.Context, id bson.ObjectID, at time.Time) error {
	_, err := m.coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, bson.D{{Key: "$set", Value: bson.D{
		{Key: "lease_until", Value: at},
	}}})
	if err != nil {
		return fmt.Errorf("outbox: release %s: %w", id.Hex(), err)
	}
	return nil
}
