package api

import (
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	invoicev1 "github.com/mafuky/invoice-hub/gen/invoicehub/invoice/v1"
	providerv1 "github.com/mafuky/invoice-hub/gen/invoicehub/provider/v1"
	settlementv1 "github.com/mafuky/invoice-hub/gen/invoicehub/settlement/v1"
	"github.com/mafuky/invoice-hub/internal/platform/money"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type invoiceInput struct {
	BizOrderNo  string        `json:"biz_order_no"`
	ProductLine string        `json:"product_line"`
	Title       string        `json:"title"`
	TaxNo       string        `json:"tax_no"`
	Amount      *money.Amount `json:"amount"`
	CustomerID  string        `json:"customer_id"`
}

func decode(c *gin.Context, v any) bool {
	d := json.NewDecoder(c.Request.Body)
	if err := d.Decode(v); err != nil {
		fail(c, 400, "invalid JSON body")
		return false
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		fail(c, 400, "expected one JSON object")
		return false
	}
	return true
}
func input(c *gin.Context) (invoiceInput, invoicev1.ProductLine, bool) {
	var in invoiceInput
	if !decode(c, &in) {
		return in, 0, false
	}
	line, ok := invoicev1.ProductLine_value["PRODUCT_LINE_"+in.ProductLine]
	if !ok || line == 0 || in.Amount == nil || strings.TrimSpace(in.BizOrderNo) == "" || strings.TrimSpace(in.Title) == "" || strings.TrimSpace(in.TaxNo) == "" || strings.TrimSpace(in.CustomerID) == "" {
		fail(c, 400, "biz_order_no, product_line (FLIGHT, TRAIN or HOTEL), title, tax_no, amount and customer_id are required")
		return in, 0, false
	}
	return in, invoicev1.ProductLine(line), true
}
func id(c *gin.Context) (int64, bool) {
	n, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || n <= 0 {
		fail(c, 400, "id must be a positive integer")
		return 0, false
	}
	return n, true
}
func (h handlers) apply(c *gin.Context) {
	in, line, ok := input(c)
	if !ok {
		return
	}
	out, err := h.invoices.Apply(c.Request.Context(), &invoicev1.ApplyRequest{BizOrderNo: in.BizOrderNo, ProductLine: line, Title: in.Title, TaxNo: in.TaxNo, Amount: in.Amount.String(), CustomerId: in.CustomerID})
	if err != nil {
		rpcError(c, err)
		return
	}
	c.JSON(202, gin.H{"invoice_id": out.GetInvoiceId(), "existed": out.GetExisted()})
}
func (h handlers) supplement(c *gin.Context) {
	in, line, ok := input(c)
	if !ok {
		return
	}
	out, err := h.invoices.Supplement(c.Request.Context(), &invoicev1.SupplementRequest{BizOrderNo: in.BizOrderNo, ProductLine: line, Title: in.Title, TaxNo: in.TaxNo, Amount: in.Amount.String(), CustomerId: in.CustomerID})
	if err != nil {
		rpcError(c, err)
		return
	}
	command(c, out)
}
func command(c *gin.Context, out *invoicev1.CommandReply) {
	c.JSON(200, gin.H{"invoice_id": out.GetInvoiceId(), "command_id": out.GetCommandId(), "status": out.GetStatus()})
}
func reason(c *gin.Context) (string, bool) {
	var in struct {
		Reason string `json:"reason"`
	}
	if !decode(c, &in) {
		return "", false
	}
	return in.Reason, true
}
func (h handlers) redFlush(c *gin.Context) {
	n, ok := id(c)
	if !ok {
		return
	}
	why, ok := reason(c)
	if !ok {
		return
	}
	out, err := h.invoices.RedFlush(c.Request.Context(), &invoicev1.RedFlushRequest{InvoiceId: n, Reason: why})
	if err != nil {
		rpcError(c, err)
		return
	}
	command(c, out)
}
func (h handlers) reissue(c *gin.Context) {
	n, ok := id(c)
	if !ok {
		return
	}
	why, ok := reason(c)
	if !ok {
		return
	}
	out, err := h.invoices.Reissue(c.Request.Context(), &invoicev1.ReissueRequest{InvoiceId: n, Reason: why})
	if err != nil {
		rpcError(c, err)
		return
	}
	command(c, out)
}
func timestamp(t *timestamppb.Timestamp) any {
	if t == nil {
		return nil
	}
	return t.AsTime().UTC().Format(time.RFC3339Nano)
}
func invoiceJSON(in *invoicev1.Invoice) (gin.H, error) {
	a, err := money.Parse(in.GetAmount())
	if err != nil {
		return nil, err
	}
	return gin.H{"invoice_id": in.GetInvoiceId(), "biz_order_no": in.GetBizOrderNo(), "product_line": strings.TrimPrefix(in.GetProductLine().String(), "PRODUCT_LINE_"), "title": in.GetTitle(), "tax_no": in.GetTaxNo(), "amount": a, "status": in.GetStatus(), "version": in.GetVersion(), "created_at": timestamp(in.GetCreatedAt()), "updated_at": timestamp(in.GetUpdatedAt())}, nil
}
func (h handlers) get(c *gin.Context) {
	n, ok := id(c)
	if !ok {
		return
	}
	out, err := h.invoices.Get(c.Request.Context(), &invoicev1.GetRequest{InvoiceId: n})
	if err != nil {
		rpcError(c, err)
		return
	}
	body, err := invoiceJSON(out)
	if err != nil {
		fail(c, 500, "invalid invoice amount from service")
		return
	}
	c.JSON(200, body)
}
func (h handlers) list(c *gin.Context) {
	var size int64
	if value, exists := c.GetQuery("page_size"); exists {
		var err error
		size, err = strconv.ParseInt(value, 10, 32)
		if err != nil || size < 0 {
			fail(c, 400, "page_size must be a non-negative integer")
			return
		}
	}
	out, err := h.invoices.List(c.Request.Context(), &invoicev1.ListRequest{Status: c.Query("status"), PageSize: int32(size), PageToken: c.Query("page_token")})
	if err != nil {
		rpcError(c, err)
		return
	}
	rows := make([]gin.H, 0, len(out.GetInvoices()))
	for _, in := range out.GetInvoices() {
		body, err := invoiceJSON(in)
		if err != nil {
			fail(c, 500, "invalid invoice amount from service")
			return
		}
		rows = append(rows, body)
	}
	c.JSON(200, gin.H{"invoices": rows, "next_page_token": out.GetNextPageToken()})
}
func (h handlers) callback(c *gin.Context) {
	raw, err := io.ReadAll(c.Request.Body)
	if err != nil {
		fail(c, 400, "invalid callback body")
		return
	}
	var in struct {
		EventSeq        int64  `json:"event_seq"`
		SupplierOrderNo string `json:"supplier_order_no"`
		Outcome         string `json:"outcome"`
		InvoiceNo       string `json:"invoice_no"`
	}
	if err := json.Unmarshal(raw, &in); err != nil {
		fail(c, 400, "invalid callback JSON")
		return
	}
	outcome, ok := providerv1.CallbackOutcome_value["CALLBACK_OUTCOME_"+in.Outcome]
	if !ok || outcome == 0 || in.EventSeq <= 0 || in.SupplierOrderNo == "" {
		fail(c, 400, "supplier_order_no, positive event_seq and outcome (SUCCEEDED or FAILED) are required")
		return
	}
	out, err := h.provider.HandleCallback(c.Request.Context(), &providerv1.CallbackRequest{Supplier: c.Param("supplier"), EventSeq: in.EventSeq, SupplierOrderNo: in.SupplierOrderNo, Outcome: providerv1.CallbackOutcome(outcome), InvoiceNo: in.InvoiceNo, RawJson: string(raw)})
	if err != nil {
		rpcError(c, err)
		return
	}
	switch out.GetResult() {
	case providerv1.CallbackResult_CALLBACK_RESULT_ACCEPTED, providerv1.CallbackResult_CALLBACK_RESULT_DUPLICATE:
		c.JSON(200, gin.H{"result": strings.TrimPrefix(out.GetResult().String(), "CALLBACK_RESULT_")})
	case providerv1.CallbackResult_CALLBACK_RESULT_UNKNOWN_ORDER:
		fail(c, 404, "unknown supplier order")
	default:
		fail(c, 500, "unknown callback result")
	}
}
func settlement(c *gin.Context, out *settlementv1.Settlement) {
	a, err := money.Parse(out.GetAmount())
	if err != nil {
		fail(c, 500, "invalid settlement amount from service")
		return
	}
	items := make([]gin.H, 0, len(out.GetItems()))
	for _, it := range out.GetItems() {
		amt, err := money.Parse(it.GetAmount())
		if err != nil {
			fail(c, 500, "invalid settlement item amount from service")
			return
		}
		checks := it.GetChecks()
		if checks == nil {
			checks = []string{}
		}
		items = append(items, gin.H{"invoice_id": it.GetInvoiceId(), "amount": amt, "verified": it.GetVerified(), "checks": checks})
	}
	c.JSON(200, gin.H{"settlement_id": out.GetSettlementId(), "period": out.GetPeriod(), "customer_id": out.GetCustomerId(), "amount": a, "status": out.GetStatus(), "version": out.GetVersion(), "items": items})
}
func (h handlers) getSettlement(c *gin.Context) {
	n, ok := id(c)
	if !ok {
		return
	}
	out, err := h.settlements.Get(c.Request.Context(), &settlementv1.GetRequest{SettlementId: n})
	if err != nil {
		rpcError(c, err)
		return
	}
	settlement(c, out)
}
func (h handlers) settlementAction(action string) gin.HandlerFunc {
	return func(c *gin.Context) {
		n, ok := id(c)
		if !ok {
			return
		}
		ctx := c.Request.Context()
		req := &settlementv1.IDRequest{SettlementId: n}
		var out *settlementv1.Settlement
		var err error
		switch action {
		case "calculate":
			out, err = h.settlements.Calculate(ctx, req)
		case "submit":
			out, err = h.settlements.SubmitReview(ctx, req)
		case "approve":
			out, err = h.settlements.Approve(ctx, req)
		case "pay":
			out, err = h.settlements.Pay(ctx, req)
		case "reject":
			why, ok := reason(c)
			if !ok {
				return
			}
			if strings.TrimSpace(why) == "" {
				fail(c, 400, "a reject reason is required")
				return
			}
			out, err = h.settlements.Reject(ctx, &settlementv1.RejectRequest{SettlementId: n, Reason: why})
		}
		if err != nil {
			rpcError(c, err)
			return
		}
		settlement(c, out)
	}
}
