package mock

import (
	"net/http"
	"sync"

	"github.com/mafuky/invoice-hub/internal/platform/money"
)

type verifyRecord struct {
	productLine string
	amount      money.Amount
	title       string
	status      string
	missing     bool
}

// verifyData is the supplier-side view of invoices this mock has issued.
type verifyData struct {
	cfg VerifyCfg
	rng *rng

	mu      sync.Mutex
	records map[string]verifyRecord
}

func newVerifyData(cfg VerifyCfg, seed uint64) *verifyData {
	return &verifyData{cfg: cfg, rng: newRNG(seed, 0x76657269), records: map[string]verifyRecord{}}
}

// register stores the record for an issued invoice, applying the configured
// mismatches once so repeated lookups stay stable.
func (v *verifyData) register(invoiceNo, productLine string, amount money.Amount, title, kind string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, ok := v.records[invoiceNo]; ok {
		return
	}
	rec := verifyRecord{productLine: productLine, amount: amount, title: title, status: "issued"}
	if kind == "RED_FLUSH" {
		rec.status = "red_flushed"
	}
	if v.rng.hit(v.cfg.AmountMismatchRate) {
		one, _ := money.Parse("1.00")
		rec.amount = amount.Add(one)
	}
	if v.rng.hit(v.cfg.TitleMismatchRate) {
		rec.title = title + " (Branch)"
	}
	if v.rng.hit(v.cfg.StatusMismatchRate) {
		if rec.status == "issued" {
			rec.status = "red_flushed"
		} else {
			rec.status = "issued"
		}
	}
	rec.missing = v.rng.hit(v.cfg.MissingRate)
	v.records[invoiceNo] = rec
}

func (v *verifyData) handle(w http.ResponseWriter, r *http.Request) {
	v.mu.Lock()
	rec, ok := v.records[r.PathValue("invoice_no")]
	v.mu.Unlock()
	if !ok || rec.missing || rec.productLine != r.PathValue("product_line") {
		writeJSON(w, http.StatusOK, map[string]any{"found": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"found":  true,
		"amount": rec.amount,
		"title":  rec.title,
		"status": rec.status,
	})
}
