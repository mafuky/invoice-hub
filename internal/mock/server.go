package mock

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/mafuky/invoice-hub/internal/platform/logx"
)

// Server wires every mock endpoint onto one HTTP handler.
type Server struct {
	mux    *http.ServeMux
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewServer builds the handler for a validated scenario. Callbacks for
// supplier X are posted to callbackBase + "/" + X.
func NewServer(sc Scenario, callbackBase string) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{mux: http.NewServeMux(), cancel: cancel}
	log := logx.New("mock-supplier")

	v := newVerifyData(sc.Verify, sc.Seed)
	suppliers := map[string]*supplier{}
	for _, c := range sc.Suppliers {
		suppliers[c.Name] = newSupplier(ctx, &s.wg, c, sc.Seed, callbackBase, v, log)
	}
	pick := func(w http.ResponseWriter, r *http.Request) *supplier {
		sup := suppliers[r.PathValue("supplier")]
		if sup == nil {
			writeErr(w, http.StatusNotFound, "unknown supplier")
		}
		return sup
	}
	customers := newSink(ctx, sc.CustomerPush, sc.Seed, 0x63757374)
	finance := newSink(ctx, sc.FinanceWriteback, sc.Seed, 0x66696e61)

	s.mux.HandleFunc("POST /suppliers/{supplier}/orders", func(w http.ResponseWriter, r *http.Request) {
		if sup := pick(w, r); sup != nil {
			sup.createOrder(w, r)
		}
	})
	s.mux.HandleFunc("GET /suppliers/{supplier}/orders/{supplier_order_no}", func(w http.ResponseWriter, r *http.Request) {
		if sup := pick(w, r); sup != nil {
			sup.queryOrder(w, r)
		}
	})
	s.mux.HandleFunc("POST /customers/{customer_id}/push", func(w http.ResponseWriter, r *http.Request) {
		customers.push(w, r, r.PathValue("customer_id"))
	})
	s.mux.HandleFunc("GET /customers/{customer_id}/received", func(w http.ResponseWriter, r *http.Request) {
		customers.list(w, r.PathValue("customer_id"))
	})
	s.mux.HandleFunc("POST /finance/writeback", func(w http.ResponseWriter, r *http.Request) {
		finance.push(w, r, "finance")
	})
	s.mux.HandleFunc("GET /finance/received", func(w http.ResponseWriter, r *http.Request) {
		finance.list(w, "finance")
	})
	s.mux.HandleFunc("GET /verify/{product_line}/{invoice_no}", v.handle)
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// Close stops pending callbacks and waits for in-flight ones to unwind.
func (s *Server) Close() {
	s.cancel()
	s.wg.Wait()
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
