//go:build lesson && integration

package app

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"sync"
	"testing"

	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	"github.com/mafuky/invoice-hub/internal/invoice/store"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/platform/mq"
	"github.com/mafuky/invoice-hub/internal/platform/testenv"
)

func TestReopen_CreatesOnePendingTaskAndOutbox(t *testing.T) {
	ctx := context.Background()
	db := testenv.OpenMySQL(t, "invoice")
	repo := store.NewRepo(db)
	svc := NewService(db, repo)

	amt, _ := money.Parse("1999.99")
	invoiceID, err := repo.InsertInvoice(ctx, db, store.NewInvoice{
		BizOrderNo: testenv.BizOrderNo(), ProductLine: "FLIGHT", Title: "Example Travel Co., Ltd.",
		TaxNo: "91310000EXAMPLE001", Amount: amt, CustomerID: "CUST-0001",
	})
	if err != nil {
		t.Fatal(err)
	}
	cmdID, err := repo.InsertCommand(ctx, db, invoiceID, domain.KindIssue, "")
	if err != nil {
		t.Fatal(err)
	}
	failedID, err := repo.InsertTask(ctx, db, cmdID, invoiceID, "air-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE tasks SET status = 'failed' WHERE id = ?`, failedID); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	newID, err := svc.Reopen(ctx, tx, invoiceID, cmdID, failedID)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var live int
	var status string
	var supplier *string
	if err := db.QueryRow(`SELECT COUNT(*) FROM tasks WHERE command_id = ? AND status NOT IN ('done','failed')`, cmdID).Scan(&live); err != nil {
		t.Fatal(err)
	}
	if live != 1 {
		t.Fatalf("live tasks for the command = %d, want 1", live)
	}
	if err := db.QueryRow(`SELECT status, supplier FROM tasks WHERE id = ?`, newID).Scan(&status, &supplier); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || supplier != nil {
		t.Fatalf("replacement task: status=%s supplier=%v, want pending and none", status, supplier)
	}
	var events int
	err = db.QueryRow(`SELECT COUNT(*) FROM outbox WHERE routing_key = 'invoice.task.created' AND JSON_EXTRACT(body, '$.payload.task_id') = ?`, newID).Scan(&events)
	if err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Fatalf("task created events for the replacement = %d, want 1", events)
	}
}

func TestRedFlush_ClosedCreatesOneCommandTaskAndEvent(t *testing.T) {
	f := seedFixture(t, domain.InvoiceClosed, "", "")
	id, err := f.svc.RedFlush(context.Background(), f.inv.ID, "ticket refunded")
	if err != nil {
		t.Fatal(err)
	}
	assertPendingGraph(t, f, f.inv.ID, id, domain.KindRedFlush, "ticket refunded")
	graphCounts(t, f.db, 1, 1, 1, 1)
}

func TestRedFlush_RefusesNonClosedWithoutWrites(t *testing.T) {
	for _, status := range []domain.InvoiceStatus{domain.InvoiceApplying, domain.InvoiceIssuing, domain.InvoiceIssued, domain.InvoiceDelivered, domain.InvoiceRedFlushing, domain.InvoiceRedFlushed, domain.InvoiceFailed} {
		t.Run(string(status), func(t *testing.T) {
			f := seedFixture(t, status, "", "")
			if _, err := f.svc.RedFlush(context.Background(), f.inv.ID, "ticket refunded"); err == nil {
				t.Fatal("accepted disallowed status")
			}
			assertInvoiceUnchanged(t, f)
			graphCounts(t, f.db, 1, 0, 0, 0)
		})
	}
}

func TestReissue_RefusesNonClosedWithoutWrites(t *testing.T) {
	for _, status := range []domain.InvoiceStatus{domain.InvoiceApplying, domain.InvoiceIssuing, domain.InvoiceIssued, domain.InvoiceDelivered, domain.InvoiceRedFlushing, domain.InvoiceRedFlushed, domain.InvoiceFailed} {
		t.Run(string(status), func(t *testing.T) {
			f := seedFixture(t, status, "", "")
			if _, err := f.svc.Reissue(context.Background(), f.inv.ID, "ticket rebooked"); err == nil {
				t.Fatal("accepted disallowed status")
			}
			assertInvoiceUnchanged(t, f)
			graphCounts(t, f.db, 1, 0, 0, 0)
		})
	}
}

func TestRedFlush_ConcurrentRequestsHaveOneActiveAttempt(t *testing.T) {
	f := seedFixture(t, domain.InvoiceClosed, "", "")
	const n = 8
	ids, errs := make([]int64, n), make([]error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ids[i], errs[i] = f.svc.RedFlush(context.Background(), f.inv.ID, "ticket refunded")
		}()
	}
	close(start)
	wg.Wait()
	var accepted int
	var id int64
	for i, err := range errs {
		if err == nil {
			accepted++
			id = ids[i]
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted=%d, want one; active duplicates must be refused", accepted)
	}
	assertPendingGraph(t, f, f.inv.ID, id, domain.KindRedFlush, "ticket refunded")
	graphCounts(t, f.db, 1, 1, 1, 1)
}

func TestSupplement_NewOrderCreatesSupplementGraph(t *testing.T) {
	f := seedFixture(t, domain.InvoiceClosed, "", "")
	in := requestInput(t)
	id, cmd, err := f.svc.Supplement(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	inv, err := f.repo.GetInvoice(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	assertFields(t, inv, in)
	if inv.Status != domain.InvoiceApplying {
		t.Fatalf("status=%s", inv.Status)
	}
	assertPendingGraph(t, f, id, cmd, domain.KindSupplement, "")
	graphCounts(t, f.db, 2, 1, 1, 1)
}

func TestSupplement_ExistingOrderRefusedWithoutWrites(t *testing.T) {
	t.Run("existing", func(t *testing.T) {
		f := seedFixture(t, domain.InvoiceIssued, domain.KindIssue, domain.TaskDone)
		beforeCommand, err := f.repo.GetCommand(context.Background(), f.db, f.cmd)
		if err != nil {
			t.Fatal(err)
		}
		beforeTask, err := f.repo.GetTask(context.Background(), f.db, f.task)
		if err != nil {
			t.Fatal(err)
		}
		in := requestInput(t)
		in.BizOrderNo = f.inv.BizOrderNo
		if _, _, err := f.svc.Supplement(context.Background(), in); !errors.Is(err, ErrBizOrderExists) {
			t.Fatalf("err=%v", err)
		}
		assertInvoiceUnchanged(t, f)
		graphCounts(t, f.db, 1, 1, 1, 0)
		countRows(t, f.db, `SELECT COUNT(*) FROM tasks WHERE id=? AND status='done'`, 1, f.task)
		countRows(t, f.db, `SELECT COUNT(*) FROM commands WHERE id=? AND kind='ISSUE'`, 1, f.cmd)
		afterCommand, err := f.repo.GetCommand(context.Background(), f.db, f.cmd)
		if err != nil {
			t.Fatal(err)
		}
		afterTask, err := f.repo.GetTask(context.Background(), f.db, f.task)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(beforeCommand, afterCommand) || !reflect.DeepEqual(beforeTask, afterTask) {
			t.Fatal("existing command or task changed")
		}
	})
	t.Run("concurrent new order", func(t *testing.T) {
		f := seedFixture(t, domain.InvoiceClosed, "", "")
		in := requestInput(t)
		const n = 8
		errs := make([]error, n)
		ids, cmds := make([]int64, n), make([]int64, n)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				ids[i], cmds[i], errs[i] = f.svc.Supplement(context.Background(), in)
			}()
		}
		close(start)
		wg.Wait()
		accepted := 0
		for i, err := range errs {
			if err == nil {
				accepted++
				assertPendingGraph(t, f, ids[i], cmds[i], domain.KindSupplement, "")
			} else if !errors.Is(err, ErrBizOrderExists) {
				t.Fatalf("duplicate: %v", err)
			}
		}
		if accepted != 1 {
			t.Fatalf("accepted=%d", accepted)
		}
		graphCounts(t, f.db, 2, 1, 1, 1)
	})
}

func TestReissue_DefersReplacementUntilRedFlushSucceeded(t *testing.T) {
	for _, outcome := range []string{mq.ProviderTaskFailed, mq.ProviderTaskSucceeded} {
		t.Run(outcome, func(t *testing.T) {
			f := seedFixture(t, domain.InvoiceClosed, "", "")
			cmd, err := f.svc.Reissue(context.Background(), f.inv.ID, "ticket rebooked")
			if err != nil {
				t.Fatal(err)
			}
			task := assertPendingGraph(t, f, f.inv.ID, cmd, domain.KindReissue, "ticket rebooked")
			graphCounts(t, f.db, 1, 1, 1, 1)
			sendEvent(t, f, mq.ProviderTaskSubmitted, task)
			graphCounts(t, f.db, 1, 1, 1, 1)
			sendEvent(t, f, outcome, task)
			if outcome == mq.ProviderTaskFailed {
				graphCounts(t, f.db, 1, 1, 1, 1)
			} else {
				graphCounts(t, f.db, 2, 2, 2, 2)
				countRows(t, f.db, `SELECT COUNT(*) FROM invoices WHERE id=? AND status='red_flushed'`, 1, f.inv.ID)
			}
		})
	}
}

func TestIssueReplacement_ReplayReturnsSameReplacement(t *testing.T) {
	f := seedFixture(t, domain.InvoiceRedFlushed, domain.KindReissue, domain.TaskDone)
	tx := beginTx(t, f.db)
	first, err := f.svc.IssueReplacement(context.Background(), tx, f.inv.ID, f.cmd)
	if err != nil {
		t.Fatal(err)
	}
	commitTx(t, tx)
	tx = beginTx(t, f.db)
	second, err := f.svc.IssueReplacement(context.Background(), tx, f.inv.ID, f.cmd)
	if err != nil {
		t.Fatal(err)
	}
	commitTx(t, tx)
	if first != second || first == f.inv.ID {
		t.Fatalf("replacement ids=%d,%d", first, second)
	}
	inv, err := f.repo.GetInvoice(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^` + regexp.QuoteMeta(f.inv.BizOrderNo) + `-R[1-9][0-9]*$`).MatchString(inv.BizOrderNo) {
		t.Fatalf("biz order=%s", inv.BizOrderNo)
	}
	in := ApplyInput{BizOrderNo: inv.BizOrderNo, ProductLine: f.inv.ProductLine, Title: f.inv.Title, TaxNo: f.inv.TaxNo, Amount: f.inv.Amount, CustomerID: f.inv.CustomerID}
	assertFields(t, inv, in)
	if inv.Status != domain.InvoiceApplying || inv.InvoiceNo != "" {
		t.Fatalf("replacement=%+v", inv)
	}
	var cmd int64
	if err := f.db.QueryRow(`SELECT id FROM commands WHERE invoice_id=?`, first).Scan(&cmd); err != nil {
		t.Fatal(err)
	}
	// Reason is not part of the replacement ISSUE contract.
	c, err := f.repo.GetCommand(context.Background(), f.db, cmd)
	if err != nil {
		t.Fatal(err)
	}
	assertPendingGraph(t, f, first, cmd, domain.KindIssue, c.Reason)
	graphCounts(t, f.db, 2, 2, 2, 1)
	countRows(t, f.db, `SELECT COUNT(*) FROM invoices WHERE id=? AND status='red_flushed'`, 1, f.inv.ID)
}

func TestIssueReplacement_RollbackLeavesNoReplacement(t *testing.T) {
	f := seedFixture(t, domain.InvoiceRedFlushed, domain.KindReissue, domain.TaskDone)
	tx := beginTx(t, f.db)
	if _, err := f.svc.IssueReplacement(context.Background(), tx, f.inv.ID, f.cmd); err != nil {
		t.Fatal(err)
	}
	graphCounts(t, tx, 2, 2, 2, 1)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	graphCounts(t, f.db, 1, 1, 1, 0)
}

func TestReopen_ConcurrentReplayOneLiveTask(t *testing.T) {
	f := seedFixture(t, domain.InvoiceIssuing, domain.KindIssue, domain.TaskFailed)
	const n = 8
	errs := make([]error, n)
	ids := make([]int64, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			tx, err := f.db.BeginTx(context.Background(), nil)
			if err != nil {
				errs[i] = err
				return
			}
			defer tx.Rollback()
			ids[i], errs[i] = f.svc.Reopen(context.Background(), tx, f.inv.ID, f.cmd, f.task)
			if errs[i] == nil {
				errs[i] = tx.Commit()
			}
		}()
	}
	close(start)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range ids {
		if id != ids[0] {
			t.Fatalf("replay ids=%v", ids)
		}
	}
	countRows(t, f.db, `SELECT COUNT(*) FROM tasks WHERE command_id=? AND status='pending' AND supplier IS NULL`, 1, f.cmd)
	assertTaskEvent(t, f.db, f.inv, ids[0], domain.KindIssue)
	graphCounts(t, f.db, 1, 1, 2, 1)
}

func TestReopen_RollbackLeavesNoTaskOrEvent(t *testing.T) {
	f := seedFixture(t, domain.InvoiceIssuing, domain.KindIssue, domain.TaskFailed)
	tx := beginTx(t, f.db)
	id, err := f.svc.Reopen(context.Background(), tx, f.inv.ID, f.cmd, f.task)
	if err != nil {
		t.Fatal(err)
	}
	assertTaskEvent(t, tx, f.inv, id, domain.KindIssue)
	graphCounts(t, tx, 1, 1, 2, 1)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	graphCounts(t, f.db, 1, 1, 1, 0)
}
