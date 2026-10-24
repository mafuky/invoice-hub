package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthz_Returns200(t *testing.T) {
	srv := httptest.NewServer(healthMux())
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}
