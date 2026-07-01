package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTangleveilFetchRoutesHonorsNetworkFilter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sources" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode([]tvSource{
			{ID: "src-a", Mapping: "net-a:0"},
			{ID: "src-b", Mapping: "net-b:0"},
		})
	}))
	defer srv.Close()

	store := NewStore([]NetworkConfig{
		{ID: "net-a", Name: "A", Analyzers: []AnalyzerConfig{{Name: "a", URL: "https://a.example"}}},
		{ID: "net-b", Name: "B", Analyzers: []AnalyzerConfig{{Name: "b", URL: "https://b.example"}}},
	})
	tc := &TangleveilCollector{
		baseURL: srv.URL,
		store:   store,
		only:    map[string]bool{"net-b": true},
	}

	routes, err := tc.fetchRoutes()
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 1 {
		t.Fatalf("routes = %d, want 1", len(routes))
	}
	if routes["src-b"] == nil {
		t.Fatalf("src-b route missing: %+v", routes)
	}
	if routes["src-a"] != nil {
		t.Fatalf("src-a route should have been filtered out: %+v", routes)
	}
}
