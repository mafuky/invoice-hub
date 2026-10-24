//go:build lesson

package verify

import (
	"testing"

	"github.com/mafuky/invoice-hub/internal/platform/money"
)

func amt(t *testing.T, s string) money.Amount {
	t.Helper()
	a, err := money.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func matching(t *testing.T) (InvoiceView, SupplierInvoice) {
	t.Helper()
	ours := InvoiceView{Amount: amt(t, "1999.99"), Title: "Example Travel Co., Ltd.", Status: "issued"}
	theirs := SupplierInvoice{Found: true, Amount: amt(t, "1999.99"), Title: "Example Travel Co., Ltd.", Status: "issued"}
	return ours, theirs
}

func byName(t *testing.T, checks []Check) map[string]Check {
	t.Helper()
	if len(checks) != 4 {
		t.Fatalf("got %d checks, want 4: %+v", len(checks), checks)
	}
	m := map[string]Check{}
	for _, c := range checks {
		m[c.Name] = c
	}
	for _, name := range []string{CheckExists, CheckAmount, CheckTitle, CheckStatus} {
		if _, ok := m[name]; !ok {
			t.Fatalf("no %q check in %+v", name, checks)
		}
	}
	return m
}

func TestVerify_FlagsFourAnomalyKinds(t *testing.T) {
	cases := []struct {
		name   string
		change func(*SupplierInvoice)
		failed string
	}{
		{"amount differs", func(s *SupplierInvoice) { s.Amount = amt(t, "2000.99") }, CheckAmount},
		{"title differs", func(s *SupplierInvoice) { s.Title = "Example Travel Co., Ltd. (Branch)" }, CheckTitle},
		{"status differs", func(s *SupplierInvoice) { s.Status = "red_flushed" }, CheckStatus},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ours, theirs := matching(t)
			c.change(&theirs)
			got := byName(t, Run("TRAIN", ours, theirs))
			for name, chk := range got {
				if want := name != c.failed; chk.Passed != want {
					t.Errorf("%s: passed=%v, want %v (%s)", name, chk.Passed, want, chk.Detail)
				}
			}
			if got[c.failed].Detail == "" {
				t.Errorf("%s failed without a detail", c.failed)
			}
		})
	}

	t.Run("not found", func(t *testing.T) {
		ours, _ := matching(t)
		got := byName(t, Run("TRAIN", ours, SupplierInvoice{}))
		for name, chk := range got {
			if chk.Passed {
				t.Errorf("%s passed for an invoice the supplier does not know", name)
			}
			if chk.Detail == "" {
				t.Errorf("%s has no detail", name)
			}
		}
	})

	t.Run("all match", func(t *testing.T) {
		ours, theirs := matching(t)
		for name, chk := range byName(t, Run("TRAIN", ours, theirs)) {
			if !chk.Passed {
				t.Errorf("%s failed on matching records: %s", name, chk.Detail)
			}
		}
	})
}

func TestVerify_HotelToleratesOneCent(t *testing.T) {
	ours := InvoiceView{Amount: amt(t, "100.00"), Title: "Example Travel Co., Ltd.", Status: "issued"}
	theirs := SupplierInvoice{Found: true, Amount: amt(t, "100.01"), Title: "Example Travel Co., Ltd.", Status: "issued"}

	if got := byName(t, Run("HOTEL", ours, theirs)); !got[CheckAmount].Passed {
		t.Errorf("hotel amount off by one cent should pass: %s", got[CheckAmount].Detail)
	}
	if got := byName(t, Run("FLIGHT", ours, theirs)); got[CheckAmount].Passed {
		t.Error("flight amount off by one cent should fail")
	}

	theirs.Amount = amt(t, "100.02")
	if got := byName(t, Run("HOTEL", ours, theirs)); got[CheckAmount].Passed {
		t.Error("hotel amount off by two cents should fail")
	}
	theirs.Amount = amt(t, "99.99")
	if got := byName(t, Run("HOTEL", ours, theirs)); !got[CheckAmount].Passed {
		t.Error("hotel tolerance applies in both directions")
	}
}

func TestVerify_FlightRequiresSupplierIssued(t *testing.T) {
	ours, theirs := matching(t)
	ours.Status = "red_flushed"
	theirs.Status = "red_flushed"
	if got := byName(t, Run("FLIGHT", ours, theirs)); got[CheckStatus].Passed {
		t.Error("flight invoice the supplier reports as red_flushed should fail the status check")
	}
	if got := byName(t, Run("TRAIN", ours, theirs)); !got[CheckStatus].Passed {
		t.Error("train only compares the two statuses")
	}
}
