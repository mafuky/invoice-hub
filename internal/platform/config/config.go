// Package config loads service settings from the environment and an optional .env file.
package config

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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
}

var keyRe = regexp.MustCompile(`^[A-Z0-9_]+$`)

// Load reads the process environment, falling back to a .env file in the
// repository root (the nearest parent directory holding go.mod).
// An empty environment variable counts as unset.
func Load() (Config, error) {
	file := map[string]string{}
	if root, ok := repoRoot(); ok {
		if m, err := parseEnvFile(filepath.Join(root, ".env")); err == nil {
			file = m
		} else if !errors.Is(err, os.ErrNotExist) {
			return Config{}, fmt.Errorf("config: read .env: %w", err)
		}
	}

	var missing []string
	get := func(key, def string, required bool) string {
		v, ok := os.LookupEnv(key)
		if !ok || v == "" {
			v = file[key]
		}
		if v == "" {
			if required {
				missing = append(missing, key)
			}
			return def
		}
		return v
	}

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
		MockAddr:       get("INVHUB_MOCK_ADDR", ":8090", false),

		MockURL:         get("INVHUB_MOCK_URL", "http://127.0.0.1:8090", false),
		MockCallbackURL: get("INVHUB_MOCK_CALLBACK_URL", "http://127.0.0.1:8080/callbacks", false),
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("config: missing required keys: %s", strings.Join(missing, ", "))
	}
	return c, nil
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

// redactURL masks the password and drops the query of a DSN or URI.
func redactURL(s string) string {
	if s == "" {
		return ""
	}
	scheme := ""
	rest := s
	if i := strings.Index(s, "://"); i >= 0 {
		scheme, rest = s[:i+3], s[i+3:]
	}
	if i := strings.IndexByte(rest, '?'); i >= 0 {
		rest = rest[:i]
	}
	at := strings.LastIndexByte(rest, '@')
	if at < 0 {
		return scheme + rest
	}
	user, _, _ := strings.Cut(rest[:at], ":")
	return scheme + user + ":***@" + rest[at+1:]
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
		slog.String("mysql_dsn_invoice", redactURL(c.MySQLDSNInvoice)),
		slog.String("mysql_dsn_provider", redactURL(c.MySQLDSNProvider)),
		slog.String("mysql_dsn_settlement", redactURL(c.MySQLDSNSettlement)),
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
