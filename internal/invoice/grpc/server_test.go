package invoicegrpc_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	invoicev1 "github.com/mafuky/invoice-hub/gen/invoicehub/invoice/v1"
	"github.com/mafuky/invoice-hub/internal/invoice/app"
	"github.com/mafuky/invoice-hub/internal/invoice/domain"
	invoicegrpc "github.com/mafuky/invoice-hub/internal/invoice/grpc"
	"github.com/mafuky/invoice-hub/internal/platform/logx"
	"github.com/mafuky/invoice-hub/internal/platform/money"
)

type fakeRepo struct {
	invoices []domain.Invoice
	lastList struct {
		status string
		after  int64
		limit  int
	}
}

func (f *fakeRepo) GetInvoice(_ context.Context, id int64) (domain.Invoice, error) {
	for _, inv := range f.invoices {
		if inv.ID == id {
			return inv, nil
		}
	}
	return domain.Invoice{}, fmt.Errorf("invoice %d: %w", id, domain.ErrNotFound)
}

func (f *fakeRepo) ListInvoices(_ context.Context, status string, after int64, limit int) ([]domain.Invoice, error) {
	f.lastList.status, f.lastList.after, f.lastList.limit = status, after, limit
	var out []domain.Invoice
	for _, inv := range f.invoices {
		if inv.ID > after && (status == "" || string(inv.Status) == status) {
			out = append(out, inv)
		}
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

// fakeCommands stands in for the use cases; every command fails like an
// unfinished lesson unless a test overrides it.
type fakeCommands struct {
	trace         string
	panicOn       bool
	supplementErr error
}

func (c *fakeCommands) Apply(ctx context.Context, _ app.ApplyInput) (int64, bool, error) {
	c.trace = logx.TraceID(ctx)
	if c.panicOn {
		panic("apply blew up")
	}
	return 0, false, fmt.Errorf("lesson 2: %w", errors.ErrUnsupported)
}

func (c *fakeCommands) RedFlush(context.Context, int64, string) (int64, error) {
	return 0, fmt.Errorf("lesson 9: %w", errors.ErrUnsupported)
}

func (c *fakeCommands) Reissue(context.Context, int64, string) (int64, error) {
	return 0, fmt.Errorf("lesson 9: %w", errors.ErrUnsupported)
}

func (c *fakeCommands) Supplement(context.Context, app.ApplyInput) (int64, int64, error) {
	if c.supplementErr != nil {
		return 0, 0, c.supplementErr
	}
	return 0, 0, fmt.Errorf("lesson 9: %w", errors.ErrUnsupported)
}

func start(t *testing.T, cmds invoicegrpc.Commands, repo invoicegrpc.Reader) invoicev1.InvoiceServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := invoicegrpc.NewGRPCServer(invoicegrpc.NewServer(cmds, repo))
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return invoicev1.NewInvoiceServiceClient(conn)
}

func applyRequest() *invoicev1.ApplyRequest {
	return &invoicev1.ApplyRequest{
		BizOrderNo:  "BIZ-20261023-0001",
		ProductLine: invoicev1.ProductLine_PRODUCT_LINE_FLIGHT,
		Title:       "Example Travel Co., Ltd.",
		TaxNo:       "91310000EXAMPLE001",
		Amount:      "1999.99",
		CustomerId:  "CUST-0001",
	}
}

func TestGRPC_ApplyReturnsUnimplementedUntilLesson2(t *testing.T) {
	cmds := &fakeCommands{}
	client := start(t, cmds, &fakeRepo{})

	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-trace-id", "trace-apply-0001")
	_, err := client.Apply(ctx, applyRequest())
	st, _ := status.FromError(err)
	if st.Code() != codes.Unimplemented {
		t.Fatalf("code = %v, want Unimplemented (err: %v)", st.Code(), err)
	}
	if !strings.Contains(st.Message(), "lesson 2") {
		t.Fatalf("message %q does not name lesson 2", st.Message())
	}
	if cmds.trace != "trace-apply-0001" {
		t.Fatalf("handler saw trace id %q, want the x-trace-id header", cmds.trace)
	}
}

func TestGRPC_RedFlushReturnsUnimplementedUntilLesson9(t *testing.T) {
	client := start(t, &fakeCommands{}, &fakeRepo{})
	_, err := client.RedFlush(context.Background(), &invoicev1.RedFlushRequest{InvoiceId: 1, Reason: "ticket refunded"})
	st, _ := status.FromError(err)
	if st.Code() != codes.Unimplemented || !strings.Contains(st.Message(), "lesson 9") {
		t.Fatalf("got %v %q", st.Code(), st.Message())
	}
}

func TestGRPC_ApplyRejectsMalformedAmount(t *testing.T) {
	client := start(t, &fakeCommands{}, &fakeRepo{})
	req := applyRequest()
	req.Amount = "19.99.9"
	_, err := client.Apply(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestGRPC_GetUnknownInvoice_NotFound(t *testing.T) {
	client := start(t, &fakeCommands{}, &fakeRepo{})
	_, err := client.Get(context.Background(), &invoicev1.GetRequest{InvoiceId: 999})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound (err: %v)", status.Code(err), err)
	}
}

func seeded(n int) *fakeRepo {
	amt, _ := money.Parse("100.10")
	r := &fakeRepo{}
	for i := 1; i <= n; i++ {
		r.invoices = append(r.invoices, domain.Invoice{
			ID: int64(i), BizOrderNo: fmt.Sprintf("BIZ-20261023-%04d", i), ProductLine: "TRAIN",
			Title: "Example Travel Co., Ltd.", TaxNo: "91310000EXAMPLE001", Amount: amt,
			Status: domain.InvoiceIssued, CreatedAt: time.Unix(1792000000, 0), UpdatedAt: time.Unix(1792000000, 0),
		})
	}
	return r
}

func TestGRPC_Get_ReturnsAmountWithTwoDecimals(t *testing.T) {
	client := start(t, &fakeCommands{}, seeded(3))
	inv, err := client.Get(context.Background(), &invoicev1.GetRequest{InvoiceId: 2})
	if err != nil {
		t.Fatal(err)
	}
	if inv.GetAmount() != "100.10" || inv.GetStatus() != "issued" || inv.GetProductLine() != invoicev1.ProductLine_PRODUCT_LINE_TRAIN {
		t.Fatalf("unexpected invoice: %v", inv)
	}
}

func TestGRPC_List_PagesWithOpaqueToken(t *testing.T) {
	repo := seeded(5)
	client := start(t, &fakeCommands{}, repo)

	first, err := client.List(context.Background(), &invoicev1.ListRequest{PageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Invoices) != 2 || first.NextPageToken == "" {
		t.Fatalf("first page: %d invoices, token %q", len(first.Invoices), first.NextPageToken)
	}
	second, err := client.List(context.Background(), &invoicev1.ListRequest{PageSize: 2, PageToken: first.NextPageToken})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Invoices) != 2 || second.Invoices[0].InvoiceId != 3 {
		t.Fatalf("second page starts at %v", second.Invoices)
	}
	last, err := client.List(context.Background(), &invoicev1.ListRequest{PageSize: 2, PageToken: second.NextPageToken})
	if err != nil {
		t.Fatal(err)
	}
	if len(last.Invoices) != 1 || last.NextPageToken != "" {
		t.Fatalf("last page: %d invoices, token %q", len(last.Invoices), last.NextPageToken)
	}
}

func TestGRPC_List_PageSizeDefaultsAndCaps(t *testing.T) {
	repo := seeded(1)
	client := start(t, &fakeCommands{}, repo)
	for _, c := range []struct {
		ask  int32
		want int
	}{{0, 51}, {-5, 51}, {10, 11}, {5000, 201}} {
		if _, err := client.List(context.Background(), &invoicev1.ListRequest{PageSize: c.ask}); err != nil {
			t.Fatal(err)
		}
		if repo.lastList.limit != c.want {
			t.Errorf("page_size %d fetched %d rows, want %d (one extra to detect a next page)", c.ask, repo.lastList.limit, c.want)
		}
	}
}

func TestGRPC_List_BadTokenAndUnknownStatus_InvalidArgument(t *testing.T) {
	client := start(t, &fakeCommands{}, seeded(1))
	if _, err := client.List(context.Background(), &invoicev1.ListRequest{PageToken: "!!not-a-token"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("bad token: %v", err)
	}
	if _, err := client.List(context.Background(), &invoicev1.ListRequest{Status: "paid"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("unknown status: %v", err)
	}
}

func TestGRPC_HandlerPanic_ReturnsInternalAndServiceKeepsServing(t *testing.T) {
	client := start(t, &fakeCommands{panicOn: true}, seeded(1))
	_, err := client.Apply(context.Background(), applyRequest())
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v, want Internal", status.Code(err))
	}
	if _, err := client.Get(context.Background(), &invoicev1.GetRequest{InvoiceId: 1}); err != nil {
		t.Fatalf("service stopped answering after a panic: %v", err)
	}
}

func TestGRPC_SupplementOnUsedBizOrderNo_AlreadyExists(t *testing.T) {
	client := start(t, &fakeCommands{supplementErr: fmt.Errorf("supplement: %w", app.ErrBizOrderExists)}, seeded(1))
	_, err := client.Supplement(context.Background(), &invoicev1.SupplementRequest{
		BizOrderNo: "BIZ-20261023-0001", ProductLine: invoicev1.ProductLine_PRODUCT_LINE_FLIGHT,
		Title: "Example Travel Co., Ltd.", TaxNo: "91310000EXAMPLE001", Amount: "1999.99", CustomerId: "CUST-0001",
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("code = %v, want AlreadyExists", status.Code(err))
	}
}
