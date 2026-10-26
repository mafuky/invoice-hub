// Package config loads service settings from the environment and an optional .env file.
package config

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

type Config struct {
	SSHHost   string
	SSHPort   string
	SSHUser   string
	RemoteDir string

	MySQLDSNInvoice    string
	MySQLDSNProvider   string
	MySQLDSNSettlement string

	RedisAddr     string
	RedisPassword string
	AMQPURL       string
	MongoURI      string
	MongoDB       string

	APIAddr        string
	InvoiceAddr    string
	ProviderAddr   string
	SettlementAddr string
	DeliveryAddr   string
	MockAddr       string

	MockURL         string
	MockCallbackURL string

	// CallbackDeadline is how long invoice-svc waits for a supplier callback before querying the order.
	CallbackDeadline time.Duration

	// DeliveryTimeout bounds one push to the customer endpoint or the finance center.
	DeliveryTimeout time.Duration
	// DeliveryStuckAfter is how long a delivery may stay in delivering before it is taken back.
	DeliveryStuckAfter time.Duration
}

var keyRe = regexp.MustCompile(`^[A-Z0-9_]+$`)

const (
	defaultMockAddr        = ":8090"
	defaultMockCallbackURL = "http://127.0.0.1:8080/callbacks"

	defaultCallbackDeadline = 2 * time.Minute

	defaultDeliveryTimeout    = 2 * time.Second
	defaultDeliveryStuckAfter = 2 * time.Minute
)

// source resolves keys from the process environment, then the .env file.
type source struct {
	file    map[string]string
	missing []string
}

func newSource() (*source, error) {
	file := map[string]string{}
	if root, ok := repoRoot(); ok {
		if m, err := parseEnvFile(filepath.Join(root, ".env")); err == nil {
			file = m
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("config: read .env: %w", err)
		}
	}
	return &source{file: file}, nil
}

func (s *source) get(key, def string, required bool) string {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		v = s.file[key]
	}
	if v == "" {
		if required {
			s.missing = append(s.missing, key)
		}
		return def
	}
	return v
}

// MockConfig is the subset of settings the mock-supplier needs.
type MockConfig struct {
	Addr        string
	CallbackURL string
}

// LoadMock reads only the mock-supplier keys, so it runs without middleware settings.
func LoadMock() (MockConfig, error) {
	src, err := newSource()
	if err != nil {
		return MockConfig{}, err
	}
	return src.mock(), nil
}

func (s *source) mock() MockConfig {
	return MockConfig{
		Addr:        s.get("INVHUB_MOCK_ADDR", defaultMockAddr, false),
		CallbackURL: s.get("INVHUB_MOCK_CALLBACK_URL", defaultMockCallbackURL, false),
	}
}

// Load reads the process environment, falling back to a .env file in the
// repository root (the nearest parent directory holding go.mod).
// An empty environment variable counts as unset.
func Load() (Config, error) {
	src, err := newSource()
	if err != nil {
		return Config{}, err
	}
	get := src.get
	mc := src.mock()

	c := Config{
		SSHHost:   get("INVHUB_SSH_HOST", "", false),
		SSHPort:   get("INVHUB_SSH_PORT", "", false),
		SSHUser:   get("INVHUB_SSH_USER", "", false),
		RemoteDir: get("INVHUB_REMOTE_DIR", "", false),

		MySQLDSNInvoice:    get("INVHUB_MYSQL_DSN_INVOICE", "", true),
		MySQLDSNProvider:   get("INVHUB_MYSQL_DSN_PROVIDER", "", true),
		MySQLDSNSettlement: get("INVHUB_MYSQL_DSN_SETTLEMENT", "", true),

		RedisAddr:     get("INVHUB_REDIS_ADDR", "", true),
		RedisPassword: get("INVHUB_REDIS_PASSWORD", "", false),
		AMQPURL:       get("INVHUB_AMQP_URL", "", true),
		MongoURI:      get("INVHUB_MONGO_URI", "", true),
		MongoDB:       get("INVHUB_MONGO_DB", "", true),

		APIAddr:        get("INVHUB_API_ADDR", ":8080", false),
		InvoiceAddr:    get("INVHUB_INVOICE_ADDR", ":9101", false),
		ProviderAddr:   get("INVHUB_PROVIDER_ADDR", ":9102", false),
		SettlementAddr: get("INVHUB_SETTLEMENT_ADDR", ":9103", false),
		DeliveryAddr:   get("INVHUB_DELIVERY_ADDR", ":9104", false),
		MockAddr:       mc.Addr,

		MockURL:         get("INVHUB_MOCK_URL", "http://127.0.0.1:8090", false),
		MockCallbackURL: mc.CallbackURL,
	}
	if len(src.missing) > 0 {
		return Config{}, fmt.Errorf("config: missing required keys: %s", strings.Join(src.missing, ", "))
	}
	c.CallbackDeadline = defaultCallbackDeadline
	if v := get("INVHUB_CALLBACK_DEADLINE", "", false); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return Config{}, errors.New("config: INVHUB_CALLBACK_DEADLINE must be a positive duration such as 2m")
		}
		c.CallbackDeadline = d
	}
	if c.DeliveryTimeout, err = positiveDuration(get, "INVHUB_DELIVERY_TIMEOUT", defaultDeliveryTimeout, "2s"); err != nil {
		return Config{}, err
	}
	if c.DeliveryStuckAfter, err = positiveDuration(get, "INVHUB_DELIVERY_STUCK_AFTER", defaultDeliveryStuckAfter, "2m"); err != nil {
		return Config{}, err
	}
	return c, nil
}

func positiveDuration(get func(key, def string, required bool) string, key string, def time.Duration, example string) (time.Duration, error) {
	v := get(key, "", false)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("config: %s must be a positive duration such as %s", key, example)
	}
	return d, nil
}

func repoRoot() (string, bool) {
	dir, err := os.Getwd()
	if err != nil {
		return "", false
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}

// parseEnvFile reads KEY=VALUE lines only; nothing is evaluated or expanded.
func parseEnvFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	out := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		k = strings.TrimSpace(k)
		if !ok || !keyRe.MatchString(k) {
			continue
		}
		v = strings.TrimSpace(v)
		if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
			v = v[1 : len(v)-1]
		}
		out[k] = v
	}
	return out, sc.Err()
}

// redactURL removes URI credentials and query parameters before logging.
func redactURL(s string) string {
	if s == "" {
		return ""
	}
	u, err := url.Parse(s)
	if err != nil || u.Scheme == "" || u.Host == "" || u.Opaque != "" {
		return "***"
	}
	u.User = nil
	u.RawQuery, u.Fragment, u.RawFragment = "", "", ""
	u.ForceQuery = false
	return u.String()
}

func redactDSN(s string) string {
	if s == "" {
		return ""
	}
	c, err := mysql.ParseDSN(s)
	if err != nil {
		return "***"
	}
	// Only retain the destination; driver options may contain credentials too.
	safe := mysql.NewConfig()
	safe.User, safe.Passwd = c.User, "***"
	safe.Net, safe.Addr, safe.DBName = c.Net, c.Addr, c.DBName
	return safe.FormatDSN()
}

func redactSecret(s string) string {
	if s == "" {
		return ""
	}
	return "***"
}

func (c Config) redacted() []slog.Attr {
	return []slog.Attr{
		slog.String("ssh_host", c.SSHHost),
		slog.String("ssh_port", c.SSHPort),
		slog.String("ssh_user", c.SSHUser),
		slog.String("remote_dir", c.RemoteDir),
		slog.String("mysql_dsn_invoice", redactDSN(c.MySQLDSNInvoice)),
		slog.String("mysql_dsn_provider", redactDSN(c.MySQLDSNProvider)),
		slog.String("mysql_dsn_settlement", redactDSN(c.MySQLDSNSettlement)),
		slog.String("redis_addr", c.RedisAddr),
		slog.String("redis_password", redactSecret(c.RedisPassword)),
		slog.String("amqp_url", redactURL(c.AMQPURL)),
		slog.String("mongo_uri", redactURL(c.MongoURI)),
		slog.String("mongo_db", c.MongoDB),
		slog.String("api_addr", c.APIAddr),
		slog.String("invoice_addr", c.InvoiceAddr),
		slog.String("provider_addr", c.ProviderAddr),
		slog.String("settlement_addr", c.SettlementAddr),
		slog.String("delivery_addr", c.DeliveryAddr),
		slog.String("mock_addr", c.MockAddr),
		slog.String("mock_url", c.MockURL),
		slog.String("mock_callback_url", c.MockCallbackURL),
		slog.String("callback_deadline", c.CallbackDeadline.String()),
		slog.String("delivery_timeout", c.DeliveryTimeout.String()),
		slog.String("delivery_stuck_after", c.DeliveryStuckAfter.String()),
	}
}

// LogValue keeps secrets out of structured logs.
func (c Config) LogValue() slog.Value { return slog.GroupValue(c.redacted()...) }

// String keeps secrets out of %v and %s output.
func (c Config) String() string {
	var b strings.Builder
	for i, a := range c.redacted() {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(a.Key + "=" + a.Value.String())
	}
	return b.String()
}
