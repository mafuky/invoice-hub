package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestOpenMongo_ValidURIFormsReachDriver(t *testing.T) {
	for _, uri := range []string{
		"mongodb://localhost:27017,localhost:27018/invoicehub",
		"mongodb+srv://cluster.example.invalid/invoicehub",
		"mongodb://localhost/invoicehub",
	} {
		t.Run(uri, func(t *testing.T) {
			called := false
			_, err := openMongo(context.Background(), uri, "invoicehub", func(got string) (*mongo.Client, error) {
				called = true
				if got != uri {
					t.Fatal("URI changed before reaching driver")
				}
				return nil, errors.New("driver connection unavailable")
			})
			if !called {
				t.Fatal("valid URI rejected before reaching driver")
			}
			if err == nil || !strings.Contains(err.Error(), "make tunnel") {
				t.Fatalf("missing tunnel hint: %v", err)
			}
		})
	}
	t.Run("malformed_redaction", func(t *testing.T) {
		for _, uri := range []string{"mongodb://app:secret%zz@localhost", "mongodb://app:secret?tail@localhost", "invalid://app:secret@localhost"} {
			_, err := OpenMongo(context.Background(), uri, "invoicehub")
			if err == nil || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), uri) {
				t.Fatal("malformed URI error missing or unsafe")
			}
		}
	})
	t.Run("connection_error_redaction", func(t *testing.T) {
		_, err := openMongo(context.Background(), "mongodb://localhost", "invoicehub", func(string) (*mongo.Client, error) {
			return nil, errors.New("password=connection-secret")
		})
		if err == nil || strings.Contains(err.Error(), "connection-secret") {
			t.Fatal("connection error missing or unsafe")
		}
	})
	t.Run("bounded_ping", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		start := time.Now()
		_, err := openMongo(ctx, "mongodb://app:ping-secret@127.0.0.1:1", "invoicehub", func(uri string) (*mongo.Client, error) {
			return mongo.Connect(options.Client().ApplyURI(uri))
		})
		if err == nil || strings.Contains(err.Error(), "ping-secret") || !strings.Contains(err.Error(), "make tunnel") {
			t.Fatalf("unsafe or unhelpful ping error: %v", err)
		}
		if time.Since(start) > time.Second {
			t.Fatal("Ping ignored cancellation")
		}
	})
}
