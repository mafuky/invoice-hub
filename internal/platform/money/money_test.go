package money

import (
	"encoding/json"
	"testing"
)

func TestAmount_RoundTripKeepsTwoDecimals(t *testing.T) {
	a, err := Parse("100.10")
	if err != nil {
		t.Fatal(err)
	}
	if a.String() != "100.10" {
		t.Fatalf("String() = %q", a.String())
	}

	b, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `"100.10"` {
		t.Fatalf("json = %s", b)
	}
	var back Amount
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if !back.Equal(a) {
		t.Fatalf("json round trip: %v != %v", back, a)
	}

	v, err := a.Value()
	if err != nil {
		t.Fatal(err)
	}
	if v != "100.10" {
		t.Fatalf("Value() = %#v", v)
	}
	var scanned Amount
	if err := scanned.Scan([]byte("100.10")); err != nil {
		t.Fatal(err)
	}
	if !scanned.Equal(a) || scanned.String() != "100.10" {
		t.Fatalf("Scan: %v", scanned)
	}
}

func TestAmount_RejectsMoreThanTwoDecimals(t *testing.T) {
	if _, err := Parse("1.005"); err == nil {
		t.Fatal("1.005 should be rejected")
	}
	if _, err := Parse("abc"); err == nil {
		t.Fatal("abc should be rejected")
	}
	if _, err := Parse("1.500"); err != nil {
		t.Fatalf("1.500: %v", err)
	}
	if _, err := Parse("-0.01"); err != nil {
		t.Fatalf("-0.01: %v", err)
	}
}

func TestAmount_ParseIsBounded(t *testing.T) {
	for _, in := range []string{"1e2", "+5", "1e999999999", "12345678901234567.00", " 1.00", "", "1.", ".5"} {
		if _, err := Parse(in); err == nil {
			t.Errorf("Parse(%q) should fail", in)
		}
	}
	for _, in := range []string{"9999999999999999.99", "-0.01", "0", "1.500"} {
		if _, err := Parse(in); err != nil {
			t.Errorf("Parse(%q): %v", in, err)
		}
	}
}
