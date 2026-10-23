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
	down := flag.Bool("down", false, "revert MySQL migrations instead of applying them")
	flag.Parse()
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
		db, err := store.OpenMySQL(ctx, s.dsn)
		if err != nil {
			return err
		}
		if down {
			err = store.MigrateMySQLDown(db, s.name)
		} else {
			err = store.MigrateMySQL(db, s.name)
		}
		db.Close()
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
