package settlementgrpc_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	settlementv1 "github.com/mafuky/invoice-hub/gen/invoicehub/settlement/v1"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"github.com/mafuky/invoice-hub/internal/settlement/domain"
	settlementgrpc "github.com/mafuky/invoice-hub/internal/settlement/grpc"
	"github.com/mafuky/invoice-hub/internal/settlement/store"
	"github.com/mafuky/invoice-hub/internal/settlement/verify"
)

type fakeReader struct {
	settlements []domain.Settlement
	items       map[int64][]store.Item
	lastStatus  string
}

func (f *fakeReader) GetSettlement(_ context.Context, id int64) (domain.Settlement, error) {
	for _, s := range f.settlements {
		if s.ID == id {
			return s, nil
		}
	}
	return domain.Settlement{}, fmt.Errorf("settlement %d: %w", id, domain.ErrNotFound)
}

func (f *fakeReader) ListSettlements(_ context.Context, status string) ([]domain.Settlement, error) {
	f.lastStatus = status
	var out []domain.Settlement
	for _, s := range f.settlements {
		if status == "" || string(s.Status) == status {
			out = append(out, s)
		}
	}
	return out, nil
}

func (f *fakeReader) ListItems(_ context.Context, id int64) ([]store.Item, error) {
	return f.items[id], nil
}

// fakeCommands fails like an unfinished lesson 7 unless err is set.
type fakeCommands struct {
	err error
}

func (c *fakeCommands) fail() error {
	if c.err != nil {
		return c.err
	}
	return fmt.Errorf("lesson 7: %w", errors.ErrUnsupported)
}

func (c *fakeCommands) Calculate(context.Context, int64) error      { return c.fail() }
func (c *fakeCommands) SubmitReview(context.Context, int64) error   { return c.fail() }
func (c *fakeCommands) Approve(context.Context, int64) error        { return c.fail() }
func (c *fakeCommands) Reject(context.Context, int64, string) error { return c.fail() }
func (c *fakeCommands) Pay(context.Context, int64) error            { return c.fail() }

func start(t *testing.T, cmds settlementgrpc.Commands, reader settlementgrpc.Reader) settlementv1.SettlementServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := settlementgrpc.NewGRPCServer(settlementgrpc.NewServer(cmds, reader))
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return settlementv1.NewSettlementServiceClient(conn)
}

func amount(s string) money.Amount {
	a, err := money.Parse(s)
	if err != nil {
		panic(err)
	}
	return a
}

func seeded() *fakeReader {
	return &fakeReader{
		settlements: []domain.Settlement{
			{ID: 1, Period: "2026-10", CustomerID: "CUST-0001", Amount: amount("300.30"), Status: domain.SettlementCalculated, Version: 2},
			{ID: 2, Period: "2026-09", CustomerID: "CUST-0001", Amount: amount("0"), Status: domain.SettlementPaid, Version: 7},
		},
		items: map[int64][]store.Item{
			1: {
				{InvoiceID: 11, Amount: amount("100.10"), Verified: true, Checks: []verify.Check{
					{Name: "exists", Passed: true}, {Name: "amount", Passed: true},
				}},
				{InvoiceID: 12, Amount: amount("200.20"), Verified: false, Checks: []verify.Check{
					{Name: "exists", Passed: true}, {Name: "amount", Passed: false, Detail: "ours 200.20, theirs 201.20"},
				}},
			},
		},
	}
}

func TestGRPC_RejectMapsToUnimplementedUntilLesson7(t *testing.T) {
	client := start(t, &fakeCommands{}, seeded())
	_, err := client.Reject(context.Background(), &settlementv1.RejectRequest{SettlementId: 1, Reason: "amount is wrong"})
	st, _ := status.FromError(err)
	if st.Code() != codes.Unimplemented || !strings.Contains(st.Message(), "lesson 7") {
		t.Fatalf("got %v %q, want Unimplemented naming lesson 7", st.Code(), st.Message())
	}
}

func TestGRPC_GetUnknown_NotFound(t *testing.T) {
	client := start(t, &fakeCommands{}, seeded())
	_, err := client.Get(context.Background(), &settlementv1.GetRequest{SettlementId: 999})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound (err: %v)", status.Code(err), err)
	}
}

func TestGRPC_RefusedTransition_FailedPrecondition(t *testing.T) {
	cmds := &fakeCommands{err: fmt.Errorf("settlement 1 is calculated, not pending_review: %w", domain.ErrRefused)}
	client := start(t, cmds, seeded())
	_, err := client.Approve(context.Background(), &settlementv1.IDRequest{SettlementId: 1})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition (err: %v)", status.Code(err), err)
	}
}

func TestGRPC_Get_ReturnsItemsWithChecks(t *testing.T) {
	client := start(t, &fakeCommands{}, seeded())
	got, err := client.Get(context.Background(), &settlementv1.GetRequest{SettlementId: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetAmount() != "300.30" || got.GetStatus() != "calculated" || got.GetPeriod() != "2026-10" {
		t.Fatalf("unexpected settlement: %v", got)
	}
	items := got.GetItems()
	if len(items) != 2 || items[0].GetAmount() != "100.10" || !items[0].GetVerified() || items[1].GetVerified() {
		t.Fatalf("unexpected items: %v", items)
	}
	if c := items[1].GetChecks(); len(c) != 2 || c[1] != "amount: failed (ours 200.20, theirs 201.20)" {
		t.Fatalf("unexpected checks: %v", c)
	}
}

func TestGRPC_List_FiltersByStatus(t *testing.T) {
	client := start(t, &fakeCommands{}, seeded())
	got, err := client.List(context.Background(), &settlementv1.ListRequest{Status: "paid"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.GetSettlements()) != 1 || got.GetSettlements()[0].GetSettlementId() != 2 {
		t.Fatalf("unexpected list: %v", got)
	}
	_, err = client.List(context.Background(), &settlementv1.ListRequest{Status: "closed"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown status: code = %v, want InvalidArgument", status.Code(err))
	}
}
