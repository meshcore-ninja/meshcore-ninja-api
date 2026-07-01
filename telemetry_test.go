package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDBFlushItemsMetric(t *testing.T) {
	m := NewMetrics()
	m.observeDBFlush("nodes", 17, 10*time.Millisecond, nil)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	m.handler().ServeHTTP(rr, req)
	body := rr.Body.String()
	if !strings.Contains(body, `meshcore_db_flush_items{op="nodes"} 17`) {
		t.Fatalf("metrics missing flush items gauge in:\n%s", body)
	}
}
