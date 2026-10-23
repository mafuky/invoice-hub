package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var required = map[string]string{
	"INVHUB_MYSQL_DSN_INVOICE":    "u:p@tcp(127.0.0.1:1)/invoice",
	"INVHUB_MYSQL_DSN_PROVIDER":   "u:p@tcp(127.0.0.1:1)/provider",
	"INVHUB_MYSQL_DSN_SETTLEMENT": "u:p@tcp(127.0.0.1:1)/settlement",
	"INVHUB_REDIS_ADDR":           "127.0.0.1:1",
	"INVHUB_AMQP_URL":             "amqp://u:p@127.0.0.1:1/",
	"INVHUB_MONGO_URI":            "mongodb://u:p@127.0.0.1:1/",
	"INVHUB_MONGO_DB":             "invoicehub",
}

// isolate moves into an empty module root so no real .env is picked up,
// and clears every INVHUB_ key the test does not set itself.
func isolate(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	for _, kv := range os.Environ() {
		if k, _, _ := strings.Cut(kv, "="); strings.HasPrefix(k, "INVHUB_") {
			t.Setenv(k, "")
			os.Unsetenv(k)
		}
	}
	return dir
}

func setRequired(t *testing.T) {
	for k, v := range required {
		t.Setenv(k, v)
	}
}

func TestConfig_LoadDefaultsAndOverrides(t *testing.T) {
	isolate(t)
	setRequired(t)

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.APIAddr != ":8080" {
		t.Fatalf("APIAddr = %q", cfg.APIAddr)
	}

	t.Setenv("INVHUB_API_ADDR", ":18080")
	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.APIAddr != ":18080" {
		t.Fatalf("APIAddr = %q", cfg.APIAddr)
	}

	t.Setenv("INVHUB_AMQP_URL", "")
	os.Unsetenv("INVHUB_AMQP_URL")
	_, err = Load()
	if err == nil || !strings.Contains(err.Error(), "INVHUB_AMQP_URL") {
		t.Fatalf("expected error naming INVHUB_AMQP_URL, got %v", err)
	}
	if strings.Contains(err.Error(), "u:p@") {
		t.Fatalf("error leaks a value: %v", err)
	}
}

func TestConfig_DotEnvFileAndPrecedence(t *testing.T) {
	dir := isolate(t)
	body := "# comment\nINVHUB_REDIS_ADDR=127.0.0.1:26379\nINVHUB_MONGO_DB=fromfile\nlower=ignored\nBAD LINE\nexport INVHUB_X=1\n"
	for k, v := range required {
		if k != "INVHUB_REDIS_ADDR" && k != "INVHUB_MONGO_DB" {
			body += k + "=" + v + "\n"
		}
	}
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(dir, "a", "b")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Chdir(sub)

	t.Setenv("INVHUB_MONGO_DB", "fromenv")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RedisAddr != "127.0.0.1:26379" {
		t.Fatalf("RedisAddr = %q", cfg.RedisAddr)
	}
	if cfg.MongoDB != "fromenv" {
		t.Fatalf("env should override .env, got %q", cfg.MongoDB)
	}
	if _, ok := os.LookupEnv("lower"); ok {
		t.Fatal("lowercase key must be ignored")
	}

	m, err := parseEnvFile(filepath.Join(dir, ".env"))
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"lower", "BAD LINE", "export INVHUB_X", "INVHUB_X", "# comment"} {
		if _, ok := m[k]; ok {
			t.Fatalf("key %q must not be parsed", k)
		}
	}
	if m["INVHUB_MONGO_DB"] != "fromfile" {
		t.Fatalf("parsed map: %v", m)
	}
}

func TestConfig_EnvAboveRepoRootNotRead(t *testing.T) {
	outer := isolate(t)
	body := ""
	for k, v := range required {
		body += k + "=" + v + "\n"
	}
	if err := os.WriteFile(filepath.Join(outer, ".env"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	inner := filepath.Join(outer, "repo")
	if err := os.MkdirAll(inner, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inner, "go.mod"), []byte("module y\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(inner)
	if _, err := Load(); err == nil {
		t.Fatal(".env above the go.mod directory must not be read")
	}
}

func TestConfig_StringRedactsSecrets(t *testing.T) {
	isolate(t)
	setRequired(t)
	t.Setenv("INVHUB_MYSQL_DSN_INVOICE", "app:s3cretpw@tcp(127.0.0.1:23306)/invoice?parseTime=true")
	t.Setenv("INVHUB_MONGO_URI", "mongodb://app:s3cretpw@127.0.0.1:37017/?authSource=admin")
	t.Setenv("INVHUB_REDIS_PASSWORD", "s3cretpw")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	out := fmt.Sprintf("%v | %s | %+v", cfg, cfg, cfg)
	if strings.Contains(out, "s3cretpw") {
		t.Fatalf("secret leaked: %s", out)
	}
	if !strings.Contains(out, "app:***@tcp(127.0.0.1:23306)/invoice") {
		t.Fatalf("unexpected redaction: %s", out)
	}
	if v := cfg.LogValue().String(); strings.Contains(v, "s3cretpw") {
		t.Fatalf("LogValue leaked: %s", v)
	}
}

func TestConfig_MissingListsAllKeys(t *testing.T) {
	isolate(t)
	_, err := Load()
	if err == nil {
		t.Fatal("expected error")
	}
	for k := range required {
		if !strings.Contains(err.Error(), k) {
			t.Fatalf("error should name %s: %v", k, err)
		}
	}
}

func TestLoadMock_NoMiddlewareKeysNeeded(t *testing.T) {
	isolate(t)
	c, err := LoadMock()
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != ":8090" || c.CallbackURL != "http://127.0.0.1:8080/callbacks" {
		t.Fatalf("defaults: %+v", c)
	}
	if _, err := Load(); err == nil {
		t.Fatal("Load must still require middleware keys")
	}
	t.Setenv("INVHUB_MOCK_ADDR", ":9999")
	if c, _ := LoadMock(); c.Addr != ":9999" {
		t.Fatalf("override: %+v", c)
	}
}
