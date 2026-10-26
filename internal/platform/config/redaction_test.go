package config

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestConfig_RedactionHandlesCredentialDelimiters(t *testing.T) {
	for _, tc := range []struct {
		name, dsn, uri string
		secrets        []string
	}{
		{"question", "app:question-secret?tail@tcp(localhost:3306)/invoice?token=query-secret", "mongodb://app:uri-secret@localhost/db?token=query-secret", []string{"question-secret", "uri-secret", "query-secret"}},
		{"at", "app:at-secret@tail@tcp(localhost:3306)/invoice?token=query-secret", "amqp://app:at-secret@tail@localhost/?token=query-secret", []string{"at-secret", "query-secret"}},
		{"scheme", "app:scheme-secret://tail@tcp(localhost:3306)/invoice?token=query-secret", "amqp://app:scheme-secret%3A%2F%2Ftail@localhost/?token=query-secret", []string{"scheme-secret", "query-secret"}},
		{"encoded", "app:plain-secret@tcp(localhost:3306)/invoice", "mongodb://app:encoded%3Fsecret%40tail@localhost/db?authMechanismProperties=query-secret", []string{"plain-secret", "encoded%3Fsecret%40tail", "encoded?secret@tail", "query-secret"}},
		{"malformed", "app:broken-secret", "mongodb://app:broken-secret%zz@localhost/?token=query-secret", []string{"broken-secret", "query-secret"}},
		{"ambiguous_uri", "app:plain-secret@tcp(localhost:3306)/invoice", "mongodb://app:question-secret?tail@localhost/db", []string{"plain-secret", "question-secret"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{MySQLDSNInvoice: tc.dsn, MySQLDSNProvider: tc.dsn, MySQLDSNSettlement: tc.dsn, MongoURI: tc.uri, AMQPURL: tc.uri}
			var buf bytes.Buffer
			slog.New(slog.NewJSONHandler(&buf, nil)).Info("settings", "config", cfg)
			for _, output := range []string{cfg.String(), buf.String()} {
				for _, secret := range tc.secrets {
					if strings.Contains(output, secret) {
						t.Errorf("output contains supplied secret %q", secret)
					}
				}
			}
		})
	}
}
