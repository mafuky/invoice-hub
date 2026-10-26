package gormx_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"

	"github.com/mafuky/invoice-hub/internal/platform/gormx"
)

func TestSession_ConcurrentCallsKeepTheirConnectionAndContext(t *testing.T) {
	first, err := sql.Open("mysql", "example:example@tcp(127.0.0.1:1)/invoice_test")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := sql.Open("mysql", "example:example@tcp(127.0.0.1:1)/invoice_test")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	sessions, err := gormx.New(first)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			conn := first
			if i%2 == 1 {
				conn = second
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := sessions.Session(ctx, conn)
			if s.Statement.ConnPool != conn || s.Statement.Context != ctx || !s.SkipDefaultTransaction {
				t.Error("session lost its caller's connection or context")
			}
		})
	}
	wg.Wait()
	if s := sessions.Session(context.Background(), first); s.Statement.ConnPool != first {
		t.Fatal("base connection changed")
	}
}
