package main

import (
	"strings"
	"testing"
)

func TestRedactEmbeddedCredentials(t *testing.T) {
	t.Setenv("INVHUB_AMQP_URL", "amqp://app:unique-secret@127.0.0.1:25672/")
	t.Setenv("INVHUB_MYSQL_ROOT_PASSWORD", "root-secret")
	got := redact("connection rejected unique-secret; root-secret")
	if strings.Contains(got, "unique-secret") || strings.Contains(got, "root-secret") {
		t.Fatal("credentials leaked")
	}
}

func TestMySQLConfigRejectsWrongDatabase(t *testing.T) {
	_, err := mysqlConfig("app:secret@tcp(127.0.0.1:23306)/unrelated", "invoice", false)
	if err == nil {
		t.Fatal("accepted a database outside the configured service")
	}
}

func TestMySQLConfigUsesTestDatabase(t *testing.T) {
	cfg, err := mysqlConfig("app:secret@tcp(127.0.0.1:23306)/invoice?parseTime=true", "invoice", true)
	if err != nil || cfg.DBName != "invoice_test" || !cfg.ParseTime {
		t.Fatal("test database configuration was not preserved")
	}
}
