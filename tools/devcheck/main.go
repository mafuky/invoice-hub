package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

func redact(message string) string {
	var secrets []string
	for _, entry := range os.Environ() {
		key, value, _ := strings.Cut(entry, "=")
		if !strings.HasPrefix(key, "INVHUB_") || value == "" {
			continue
		}
		if strings.Contains(key, "PASSWORD") || strings.Contains(key, "KEYFILE") {
			secrets = append(secrets, value)
		}
		if strings.Contains(key, "DSN") {
			secrets = append(secrets, value)
			if cfg, err := mysql.ParseDSN(value); err == nil && cfg.Passwd != "" {
				secrets = append(secrets, cfg.Passwd)
			}
		}
		if strings.HasSuffix(key, "URL") || strings.HasSuffix(key, "URI") {
			secrets = append(secrets, value)
			if u, err := url.Parse(value); err == nil && u.User != nil {
				if password, ok := u.User.Password(); ok && password != "" {
					secrets = append(secrets, password, url.QueryEscape(password))
				}
			}
		}
	}
	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	for _, secret := range secrets {
		message = strings.ReplaceAll(message, secret, "[redacted]")
	}
	return message
}

func mysqlConfig(dsn, database string, test bool) (*mysql.Config, error) {
	if dsn == "" {
		return nil, errors.New("missing MySQL DSN")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	if cfg.DBName != database {
		return nil, errors.New("DSN database does not match its service")
	}
	if test {
		cfg.DBName += "_test"
	}
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 10*time.Second, 10*time.Second, 10*time.Second
	return cfg, nil
}

func checkMySQL(ctx context.Context, database string, test bool) error {
	cfg, err := mysqlConfig(os.Getenv("INVHUB_MYSQL_DSN_"+strings.ToUpper(database)), database, test)
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	var result int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result); err != nil {
		return err
	}
	if result != 1 {
		return errors.New("unexpected SELECT result")
	}
	return nil
}

func checkRedis(ctx context.Context) error {
	addr, password := os.Getenv("INVHUB_REDIS_ADDR"), os.Getenv("INVHUB_REDIS_PASSWORD")
	if addr == "" || password == "" {
		return errors.New("missing Redis configuration")
	}
	client := redis.NewClient(&redis.Options{Addr: addr, Password: password, DialTimeout: 10 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, MaxRetries: -1})
	defer client.Close()
	result, err := client.Ping(ctx).Result()
	if err == nil && result != "PONG" {
		return errors.New("unexpected PING result")
	}
	return err
}

func checkAMQP(ctx context.Context) error {
	if os.Getenv("INVHUB_AMQP_URL") == "" {
		return errors.New("missing INVHUB_AMQP_URL")
	}
	conn, err := amqp.DialConfig(os.Getenv("INVHUB_AMQP_URL"), amqp.Config{
		Dial: func(network, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, address)
			if err == nil {
				deadline, _ := ctx.Deadline()
				err = conn.SetDeadline(deadline)
				if err != nil {
					conn.Close()
				}
			}
			return conn, err
		},
	})
	if err != nil {
		return err
	}
	return conn.Close()
}

func checkManagement(ctx context.Context) error {
	u, err := url.Parse(os.Getenv("INVHUB_AMQP_URL"))
	if err != nil || u.User == nil {
		return errors.New("missing AMQP credentials")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:35672/api/overview", nil)
	if err != nil {
		return err
	}
	password, _ := u.User.Password()
	request.SetBasicAuth(u.User.Username(), password)
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil},
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("management HTTP %d", response.StatusCode)
	}
	return nil
}

func mongoClient() (*mongo.Client, error) {
	uri := os.Getenv("INVHUB_MONGO_URI")
	if uri == "" {
		return nil, errors.New("missing INVHUB_MONGO_URI")
	}
	return mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(15 * time.Second).SetConnectTimeout(10 * time.Second).SetTimeout(20 * time.Second))
}

func checkMongo(ctx context.Context, transaction bool) (err error) {
	client, err := mongoClient()
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = errors.Join(err, client.Disconnect(cleanup))
	}()
	if err = client.Ping(ctx, readpref.Primary()); err != nil || !transaction {
		return err
	}
	if os.Getenv("INVHUB_MONGO_DB") != "invoicehub" {
		return errors.New("INVHUB_MONGO_DB must be invoicehub")
	}
	db := client.Database("invoicehub")
	name := "_devcheck_" + bson.NewObjectID().Hex()
	collection := db.Collection(name)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if dropErr := collection.Drop(cleanup); dropErr != nil {
			err = errors.Join(err, fmt.Errorf("cleanup %s: %w", name, dropErr))
		}
	}()
	if err = db.CreateCollection(ctx, name); err != nil {
		return fmt.Errorf("create probe collection: %w", err)
	}
	session, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer session.EndSession(context.Background())
	_, err = session.WithTransaction(ctx, func(tx context.Context) (any, error) {
		if _, e := collection.InsertOne(tx, bson.M{"_id": "first", "value": 1}); e != nil {
			return nil, fmt.Errorf("insert first document: %w", e)
		}
		return collection.InsertOne(tx, bson.M{"_id": "second", "value": 2})
	}, options.Transaction().SetWriteConcern(writeconcern.Majority()))
	if err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	count, err := collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("read committed documents: %w", err)
	}
	if count != 2 {
		return fmt.Errorf("transaction committed %d documents, expected 2", count)
	}
	abort := errors.New("deliberate transaction abort")
	_, err = session.WithTransaction(ctx, func(tx context.Context) (any, error) {
		if _, e := collection.InsertOne(tx, bson.M{"_id": "aborted-first"}); e != nil {
			return nil, e
		}
		if _, e := collection.InsertOne(tx, bson.M{"_id": "aborted-second"}); e != nil {
			return nil, e
		}
		return nil, abort
	})
	if !errors.Is(err, abort) {
		return fmt.Errorf("abort transaction: %v", err)
	}
	count, err = collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("read after rollback: %w", err)
	}
	if count != 2 {
		return errors.New("aborted transaction left documents behind")
	}
	return nil
}

func main() {
	failed := false
	check := func(name string, run func(context.Context) error) {
		started := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		if err := run(ctx); err != nil {
			fmt.Printf("✗ %s: %s\n", name, redact(err.Error()))
			failed = true
		} else {
			fmt.Printf("✓ %s (%s)\n", name, time.Since(started).Round(time.Millisecond))
		}
	}
	for _, database := range []string{"invoice", "provider", "settlement"} {
		check("MySQL "+database+" SELECT 1", func(ctx context.Context) error { return checkMySQL(ctx, database, false) })
		check("MySQL "+database+"_test SELECT 1", func(ctx context.Context) error { return checkMySQL(ctx, database, true) })
	}
	check("Redis PING", checkRedis)
	check("AMQP handshake", checkAMQP)
	check("RabbitMQ management HTTP", checkManagement)
	check("MongoDB ping", func(ctx context.Context) error { return checkMongo(ctx, false) })
	check("MongoDB multi-document commit / rollback / cleanup", func(ctx context.Context) error { return checkMongo(ctx, true) })
	if failed {
		os.Exit(1)
	}
}
