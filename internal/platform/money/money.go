// Package money provides a fixed two-decimal amount backed by decimal.
package money

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"

	"github.com/shopspring/decimal"
)

type Amount struct {
	d decimal.Decimal
}

// Parse accepts at most two decimal places; it never rounds.
func Parse(s string) (Amount, error) {
	d, err := decimal.NewFromString(s)
	if err != nil {
		return Amount{}, fmt.Errorf("money: invalid amount %q: %w", s, err)
	}
	if !d.Round(2).Equal(d) {
		return Amount{}, fmt.Errorf("money: %q has more than 2 decimal places", s)
	}
	return Amount{d: d}, nil
}

func (a Amount) String() string { return a.d.StringFixed(2) }

func (a Amount) Add(b Amount) Amount { return Amount{d: a.d.Add(b.d)} }
func (a Amount) Sub(b Amount) Amount { return Amount{d: a.d.Sub(b.d)} }
func (a Amount) Equal(b Amount) bool { return a.d.Equal(b.d) }
func (a Amount) Cmp(b Amount) int    { return a.d.Cmp(b.d) }

func (a Amount) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.String())
}

func (a *Amount) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("money: amount must be a JSON string: %w", err)
	}
	v, err := Parse(s)
	if err != nil {
		return err
	}
	*a = v
	return nil
}

func (a Amount) Value() (driver.Value, error) { return a.String(), nil }

func (a *Amount) Scan(src any) error {
	var s string
	switch v := src.(type) {
	case []byte:
		s = string(v)
	case string:
		s = v
	default:
		return fmt.Errorf("money: cannot scan %T", src)
	}
	p, err := Parse(s)
	if err != nil {
		return err
	}
	*a = p
	return nil
}
