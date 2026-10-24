//go:build integration

// Package storetest gives integration tests MongoDB collections of their own:
// every test works in collections named with a random prefix and drops them
// when it ends, so the shared collections are never touched.
package storetest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/mafuky/invoice-hub/internal/delivery/store"
	"github.com/mafuky/invoice-hub/internal/platform/config"
	platformstore "github.com/mafuky/invoice-hub/internal/platform/store"
)

var collections = []string{"deliveries", "outbox", "processed_events", "callbacks"}

// Open connects to the configured MongoDB and returns the database with a
// fresh collection prefix whose indexes are created. It skips the test when
// the config or the server is unavailable.
func Open(t *testing.T) (*mongo.Database, string) {
	t.Helper()
	cfg, err := config.Load()
	if err != nil {
		t.Skipf("no config (copy .env.example to .env and run make tunnel): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := platformstore.OpenMongo(ctx, cfg.MongoURI, cfg.MongoDB)
	if err != nil {
		t.Skipf("mongo unavailable: %v", err)
	}
	prefix := fmt.Sprintf("t%08x_", rand.Uint32())
	t.Cleanup(func() {
		dctx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		for _, name := range collections {
			_ = db.Collection(prefix + name).Drop(dctx)
		}
		_ = db.Client().Disconnect(dctx)
	})
	if err := platformstore.EnsureMongoIndexesWithPrefix(ctx, db, prefix); err != nil {
		t.Fatalf("indexes: %v", err)
	}
	return db, prefix
}

// NewRepo is a Repo over the test collections. It also skips the test when
// the server cannot run transactions (no replica set).
func NewRepo(t *testing.T) *store.Repo {
	t.Helper()
	db, prefix := Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var hello struct {
		SetName string `bson:"setName"`
		Msg     string `bson:"msg"`
	}
	if err := db.RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		t.Skipf("cannot query mongo topology: %v", err)
	}
	if hello.SetName == "" && hello.Msg != "isdbgrid" {
		t.Skip("mongo is not a replica set, so it cannot run transactions")
	}
	return store.NewRepoWithPrefix(db, prefix)
}
