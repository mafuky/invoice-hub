package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	deliverystore "github.com/mafuky/invoice-hub/internal/delivery/store"
	"github.com/mafuky/invoice-hub/internal/platform/config"
	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
	platformstore "github.com/mafuky/invoice-hub/internal/platform/store"
	"github.com/mafuky/invoice-hub/internal/reconcile"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func main() { os.Exit(execute()) }
func execute() int {
	sinceFlag := flag.String("since", "", "include invoices created at or after this RFC3339 time")
	flag.Parse()
	since, err := time.Parse(time.RFC3339Nano, *sinceFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, "reconcile: -since must be an RFC3339 timestamp")
		return 2
	}
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "reconcile: configuration could not be loaded; check INVHUB settings")
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	now := time.Now().UTC()
	snapshot, err := collect(ctx, cfg, since.UTC(), now)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	report := reconcile.Count(snapshot, now)
	fmt.Println(report.String())
	return report.ExitCode()
}
func collect(ctx context.Context, cfg config.Config, since, now time.Time) (reconcile.Snapshot, error) {
	var s reconcile.Snapshot
	var dbs []*sql.DB
	defer func() {
		for _, db := range dbs {
			_ = db.Close()
		}
	}()
	for i, dsn := range []string{cfg.MySQLDSNInvoice, cfg.MySQLDSNProvider, cfg.MySQLDSNSettlement} {
		db, err := platformstore.OpenMySQL(ctx, dsn)
		if err != nil {
			return s, fmt.Errorf("reconcile: %s database unavailable; run make tunnel", []string{"invoice", "provider", "settlement"}[i])
		}
		dbs = append(dbs, db)
	}
	mongoDB, err := platformstore.OpenMongo(ctx, cfg.MongoURI, cfg.MongoDB)
	if err != nil {
		return s, errors.New("reconcile: MongoDB unavailable; run make tunnel")
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mongoDB.Client().Disconnect(closeCtx)
	}()
	if s.Invoices, err = loadInvoices(ctx, dbs[0], since); err != nil {
		return s, err
	}
	for _, db := range dbs {
		rows, err := db.QueryContext(ctx, "SELECT status, created_at FROM outbox WHERE status = 'new' AND created_at < ?", now.Add(-time.Minute))
		if err != nil {
			return s, errors.New("reconcile: could not read MySQL outbox")
		}
		for rows.Next() {
			var o reconcile.Outbox
			if err := rows.Scan(&o.Status, &o.CreatedAt); err != nil {
				_ = rows.Close()
				return s, errors.New("reconcile: invalid MySQL outbox row")
			}
			s.Outbox = append(s.Outbox, o)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return s, errors.New("reconcile: MySQL outbox read interrupted")
		}
	}
	for offset := 0; offset < len(s.Invoices); offset += 500 {
		end := min(offset+500, len(s.Invoices))
		ids := make([]int64, 0, end-offset)
		args := make([]any, 0, end-offset)
		for _, in := range s.Invoices[offset:end] {
			ids = append(ids, in.ID)
			args = append(args, in.ID)
		}
		rows, err := dbs[2].QueryContext(ctx, "SELECT invoice_id, amount FROM settlement_items WHERE invoice_id IN ("+strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")+")", args...)
		if err != nil {
			return s, errors.New("reconcile: could not read settlement items")
		}
		for rows.Next() {
			var it reconcile.Item
			if err := rows.Scan(&it.InvoiceID, &it.Amount); err != nil {
				_ = rows.Close()
				return s, errors.New("reconcile: invalid settlement item")
			}
			s.Items = append(s.Items, it)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return s, errors.New("reconcile: settlement item read interrupted")
		}
		if err := loadDeliveries(ctx, mongoDB, ids, &s); err != nil {
			return s, err
		}
	}
	cur, err := mongoDB.Collection("outbox").Find(ctx, bson.M{"status": bson.M{"$in": []string{"new", "pending"}}, "created_at": bson.M{"$lt": now.Add(-time.Minute)}})
	if err != nil {
		return s, errors.New("reconcile: could not read Mongo outbox")
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var row struct {
			Status    string    `bson:"status"`
			CreatedAt time.Time `bson:"created_at"`
		}
		if err := cur.Decode(&row); err != nil {
			return s, errors.New("reconcile: invalid Mongo outbox document")
		}
		s.Outbox = append(s.Outbox, reconcile.Outbox{Status: row.Status, CreatedAt: row.CreatedAt})
	}
	if cur.Err() != nil {
		return s, errors.New("reconcile: Mongo outbox read interrupted")
	}
	s.Receipts, err = loadReceipts(ctx, &http.Client{Timeout: 10 * time.Second}, cfg.MockURL, s.Invoices)
	return s, err
}
func loadInvoices(ctx context.Context, db *sql.DB, since time.Time) ([]reconcile.Invoice, error) {
	rows, err := db.QueryContext(ctx, "SELECT id, customer_id, status, amount FROM invoices WHERE created_at >= ? ORDER BY id", since)
	if err != nil {
		return nil, errors.New("reconcile: could not read invoices")
	}
	invoices := []reconcile.Invoice{}
	indices := map[int64]int{}
	for rows.Next() {
		var in reconcile.Invoice
		if err := rows.Scan(&in.ID, &in.CustomerID, &in.Status, &in.Amount); err != nil {
			_ = rows.Close()
			return nil, errors.New("reconcile: invalid invoice row")
		}
		indices[in.ID] = len(invoices)
		invoices = append(invoices, in)
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return nil, errors.New("reconcile: invoice read interrupted")
	}
	// Sent issued events retain the delivery identity after later state transitions.
	rows, err = db.QueryContext(ctx, "SELECT body FROM outbox WHERE routing_key = ? AND created_at >= ?", mq.InvoiceIssued, since)
	if err != nil {
		return nil, errors.New("reconcile: could not read issued invoice versions")
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		var env event.Envelope
		var issued event.InvoiceIssued
		if err := rows.Scan(&raw); err != nil {
			return nil, errors.New("reconcile: invalid issued event row")
		}
		if err := json.Unmarshal(raw, &env); err != nil {
			return nil, errors.New("reconcile: invalid issued event envelope")
		}
		if err := env.Decode(&issued); err != nil {
			return nil, errors.New("reconcile: invalid issued event payload")
		}
		if index, ok := indices[issued.InvoiceID]; ok {
			invoices[index].Versions = append(invoices[index].Versions, issued.Version)
		}
	}
	if rows.Err() != nil {
		return nil, errors.New("reconcile: issued event read interrupted")
	}
	return invoices, nil
}
func loadDeliveries(ctx context.Context, db *mongo.Database, ids []int64, s *reconcile.Snapshot) error {
	cur, err := db.Collection("deliveries").Find(ctx, bson.M{"invoice_id": bson.M{"$in": ids}})
	if err != nil {
		return errors.New("reconcile: could not read deliveries")
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var d deliverystore.Delivery
		if err := cur.Decode(&d); err != nil {
			return errors.New("reconcile: invalid delivery document")
		}
		s.Deliveries = append(s.Deliveries, reconcile.Delivery{InvoiceID: d.InvoiceID, Version: d.InvoiceVersion, Channel: d.Channel, Status: d.Status})
	}
	if cur.Err() != nil {
		return errors.New("reconcile: delivery read interrupted")
	}
	return nil
}
func loadReceipts(ctx context.Context, client *http.Client, base string, invoices []reconcile.Invoice) ([]reconcile.Receipt, error) {
	customers := map[string]bool{}
	selected := map[int64]string{}
	for _, in := range invoices {
		customers[in.CustomerID] = true
		selected[in.ID] = in.CustomerID
	}
	type endpoint struct{ path, channel, customer string }
	endpoints := []endpoint{{"/finance/received", "finance", ""}}
	for customer := range customers {
		endpoints = append(endpoints, endpoint{"/customers/" + url.PathEscape(customer) + "/received", "customer", customer})
	}
	receipts := []reconcile.Receipt{}
	for _, ep := range endpoints {
		req, err := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(base, "/")+ep.path, nil)
		if err != nil {
			return nil, errors.New("reconcile: invalid mock address")
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, errors.New("reconcile: mock receipts unavailable")
		}
		if resp.StatusCode != 200 {
			_ = resp.Body.Close()
			return nil, errors.New("reconcile: mock receipts returned an error")
		}
		var body struct {
			Pushes []struct {
				InvoiceID string `json:"invoice_id"`
				Version   int    `json:"invoice_version"`
			} `json:"pushes"`
		}
		err = json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&body)
		_ = resp.Body.Close()
		if err != nil {
			return nil, errors.New("reconcile: invalid mock receipts")
		}
		for _, push := range body.Pushes {
			id, err := strconv.ParseInt(push.InvoiceID, 10, 64)
			if err != nil {
				return nil, errors.New("reconcile: invalid mock receipt invoice id")
			}
			customer, ok := selected[id]
			if !ok || (ep.channel == "customer" && customer != ep.customer) {
				continue
			}
			receipts = append(receipts, reconcile.Receipt{InvoiceID: id, Version: push.Version, Channel: ep.channel})
		}
	}
	return receipts, nil
}
