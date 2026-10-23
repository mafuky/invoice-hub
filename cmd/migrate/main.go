// Command migrate applies (or with -down reverts) the MySQL schemas and
// ensures the MongoDB indexes.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/mafuky/invoice-hub/internal/platform/config"
	"github.com/mafuky/invoice-hub/internal/platform/store"
)

func main() {
	down := flag.Bool("down", false, "revert MySQL migrations instead of applying them (drops tables)")
	yes := flag.Bool("yes", false, "confirm -down")
	flag.Parse()
	if *down && !*yes {
		fmt.Fprintln(os.Stderr, "migrate: -down drops every table; pass -yes to confirm")
		os.Exit(2)
	}
	if err := run(*down); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run(down bool) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	sets := []struct{ name, dsn string }{
		{"invoice", cfg.MySQLDSNInvoice},
		{"provider", cfg.MySQLDSNProvider},
		{"settlement", cfg.MySQLDSNSettlement},
	}
	for _, s := range sets {
		var err error
		if down {
			err = store.MigrateMySQLDown(ctx, s.dsn, s.name)
		} else {
			err = store.MigrateMySQL(ctx, s.dsn, s.name)
		}
		if err != nil {
			return err
		}
		fmt.Printf("mysql %s: ok\n", s.name)
	}
	if down {
		return nil
	}

	mdb, err := store.OpenMongo(ctx, cfg.MongoURI, cfg.MongoDB)
	if err != nil {
		return err
	}
	defer mdb.Client().Disconnect(context.Background())
	if err := store.EnsureMongoIndexes(ctx, mdb); err != nil {
		return err
	}
	fmt.Println("mongo indexes: ok")
	return nil
}
