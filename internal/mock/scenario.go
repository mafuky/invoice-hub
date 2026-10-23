// Package mock implements the fake suppliers, customer endpoint, finance
// center and invoice-verification source used for local runs and tests.
package mock

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

var validProductLines = map[string]bool{"FLIGHT": true, "TRAIN": true, "HOTEL": true}

type Scenario struct {
	Suppliers        []SupplierCfg `yaml:"suppliers"`
	CustomerPush     SinkCfg       `yaml:"customer_push"`
	FinanceWriteback SinkCfg       `yaml:"finance_writeback"`
	Verify           VerifyCfg     `yaml:"verify"`
	Seed             uint64        `yaml:"seed"`
}

type SupplierCfg struct {
	Name             string   `yaml:"name"`
	ProductLines     []string `yaml:"product_lines"`
	OrderLatencyMS   [2]int   `yaml:"order_latency_ms"`
	OrderFailRate    float64  `yaml:"order_fail_rate"`
	CallbackDelayMS  [2]int   `yaml:"callback_delay_ms"`
	CallbackFailRate float64  `yaml:"callback_fail_rate"`
	DuplicateRate    float64  `yaml:"duplicate_rate"`
	OutOfOrderRate   float64  `yaml:"out_of_order_rate"`
	NoCallbackRate   float64  `yaml:"no_callback_rate"`
}

type SinkCfg struct {
	FailRate        float64 `yaml:"fail_rate"`
	SlowSuccessRate float64 `yaml:"slow_success_rate"`
	// SlowDelayMS is how long a slow success holds the response after
	// recording the request; it should exceed the caller's timeout.
	SlowDelayMS int `yaml:"slow_delay_ms"`
}

type VerifyCfg struct {
	AmountMismatchRate float64 `yaml:"amount_mismatch_rate"`
	TitleMismatchRate  float64 `yaml:"title_mismatch_rate"`
	StatusMismatchRate float64 `yaml:"status_mismatch_rate"`
	MissingRate        float64 `yaml:"missing_rate"`
}

const defaultSlowDelayMS = 3000

// LoadScenario reads and validates a scenario file. Unknown keys are errors.
func LoadScenario(path string) (Scenario, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Scenario{}, fmt.Errorf("scenario: %w", err)
	}
	var sc Scenario
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&sc); err != nil && !errors.Is(err, io.EOF) {
		return Scenario{}, fmt.Errorf("scenario: parse %s: %w", path, err)
	}
	if err := sc.Validate(); err != nil {
		return Scenario{}, err
	}
	return sc, nil
}

// Validate reports the first problem found, naming the offending field.
func (sc Scenario) Validate() error {
	if len(sc.Suppliers) == 0 {
		return errors.New("scenario: suppliers must not be empty")
	}
	seen := map[string]bool{}
	for i, s := range sc.Suppliers {
		at := fmt.Sprintf("suppliers[%d]", i)
		if s.Name == "" {
			return fmt.Errorf("scenario: %s.name is required", at)
		}
		if seen[s.Name] {
			return fmt.Errorf("scenario: %s.name %q is duplicated", at, s.Name)
		}
		seen[s.Name] = true
		if len(s.ProductLines) == 0 {
			return fmt.Errorf("scenario: %s.product_lines must not be empty", at)
		}
		for _, pl := range s.ProductLines {
			if !validProductLines[pl] {
				return fmt.Errorf("scenario: %s.product_lines has unknown product line %q", at, pl)
			}
		}
		if err := checkRange(at+".order_latency_ms", s.OrderLatencyMS); err != nil {
			return err
		}
		if err := checkRange(at+".callback_delay_ms", s.CallbackDelayMS); err != nil {
			return err
		}
		rates := []struct {
			name string
			v    float64
		}{
			{"order_fail_rate", s.OrderFailRate},
			{"callback_fail_rate", s.CallbackFailRate},
			{"duplicate_rate", s.DuplicateRate},
			{"out_of_order_rate", s.OutOfOrderRate},
			{"no_callback_rate", s.NoCallbackRate},
		}
		for _, r := range rates {
			if err := checkRate(at+"."+r.name, r.v); err != nil {
				return err
			}
		}
	}
	for _, p := range []struct {
		name string
		c    SinkCfg
	}{{"customer_push", sc.CustomerPush}, {"finance_writeback", sc.FinanceWriteback}} {
		if err := checkRate(p.name+".fail_rate", p.c.FailRate); err != nil {
			return err
		}
		if err := checkRate(p.name+".slow_success_rate", p.c.SlowSuccessRate); err != nil {
			return err
		}
		if p.c.SlowDelayMS < 0 {
			return fmt.Errorf("scenario: %s.slow_delay_ms must not be negative", p.name)
		}
	}
	for _, r := range []struct {
		name string
		v    float64
	}{
		{"verify.amount_mismatch_rate", sc.Verify.AmountMismatchRate},
		{"verify.title_mismatch_rate", sc.Verify.TitleMismatchRate},
		{"verify.status_mismatch_rate", sc.Verify.StatusMismatchRate},
		{"verify.missing_rate", sc.Verify.MissingRate},
	} {
		if err := checkRate(r.name, r.v); err != nil {
			return err
		}
	}
	return nil
}

func checkRate(field string, v float64) error {
	if v < 0 || v > 1 {
		return fmt.Errorf("scenario: %s must be within [0,1], got %v", field, v)
	}
	return nil
}

func checkRange(field string, r [2]int) error {
	if r[0] < 0 || r[1] < r[0] {
		return fmt.Errorf("scenario: %s must satisfy 0 <= min <= max, got [%d, %d]", field, r[0], r[1])
	}
	return nil
}

// rng is a mutex-guarded seeded generator so one scenario seed gives one
// reproducible stream per component.
type rng struct {
	mu sync.Mutex
	r  *rand.Rand
}

func newRNG(seed, stream uint64) *rng {
	return &rng{r: rand.New(rand.NewPCG(seed, stream))}
}

// hit reports whether an event with probability p fires.
func (g *rng) hit(p float64) bool {
	if p <= 0 {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.r.Float64() < p
}

func (g *rng) between(r [2]int) time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := r[0]
	if r[1] > r[0] {
		n += g.r.IntN(r[1] - r[0] + 1)
	}
	return time.Duration(n) * time.Millisecond
}

func (g *rng) intn(n int) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.r.IntN(n)
}
