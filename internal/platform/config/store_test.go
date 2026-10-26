package config

import (
	"strings"
	"testing"
)

func TestConfig_StoreImpl(t *testing.T) {
	for _, value := range []string{"", "sql", "gorm", "GORM", "other", " sql"} {
		t.Run("value="+value, func(t *testing.T) {
			isolate(t)
			setRequired(t)
			t.Setenv("INVHUB_STORE_IMPL", value)
			cfg, err := Load()
			if value != "" && value != "sql" && value != "gorm" {
				if err == nil || !strings.Contains(err.Error(), "INVHUB_STORE_IMPL") {
					t.Fatalf("invalid store implementation accepted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := value
			if want == "" {
				want = "sql"
			}
			if cfg.StoreImpl != want {
				t.Fatalf("StoreImpl=%q want=%q", cfg.StoreImpl, want)
			}
		})
	}
}
