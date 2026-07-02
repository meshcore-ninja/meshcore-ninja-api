package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestCaretPrefixQuery covers the "^<hex>" pubkey-prefix syntax parsing.
func TestCaretPrefixQuery(t *testing.T) {
	cases := []struct {
		in      string
		wantHex string
		wantOK  bool
	}{
		{"^25", "25", true},
		{"^AB", "ab", true}, // upper-cased hex is normalised
		{"^252525", "252525", true},
		{"^2", "", false},     // fewer than 2 hex digits
		{"^", "", false},      // caret only
		{"^2g", "", false},    // non-hex digit
		{"25", "", false},     // no caret
		{"prague", "", false}, // plain name query
		{"^ 25", "", false},   // space is not hex
	}
	for _, c := range cases {
		hex, ok := caretPrefixQuery(c.in)
		if ok != c.wantOK || hex != c.wantHex {
			t.Errorf("caretPrefixQuery(%q) = (%q, %v), want (%q, %v)", c.in, hex, ok, c.wantHex, c.wantOK)
		}
	}
}

// TestPrefixOnlySearchIgnoresName confirms a PrefixOnly query matches on pubkey
// prefix alone: nodes whose key starts with the hex are returned, and a node
// that only matches by name is excluded (unlike a normal substring query).
func TestPrefixOnlySearchIgnoresName(t *testing.T) {
	r := newNodeRegistry(defaultAdvertsPerNode)
	r.Observe(AdvertObservation{PubKey: "25aa01", Name: "Alpha", At: 100, NetworkID: "net"})
	r.Observe(AdvertObservation{PubKey: "25bb02", Name: "Beta", At: 200, NetworkID: "net"})
	// Name contains "25" but the pubkey does not — must be excluded in prefix mode.
	r.Observe(AdvertObservation{PubKey: "ff9901", Name: "Room 25", At: 300, NetworkID: "net"})
	s := &Server{nodes: r}

	// Sanity: a normal query hits the name match too.
	if _, total, _ := s.mergedSearch(MapParams{Q: "25"}, 50); total != 3 {
		t.Fatalf("normal q=25 total = %d, want 3 (2 pubkey + 1 name)", total)
	}

	results, total, _ := s.mergedSearch(MapParams{Q: "25", PrefixOnly: true}, 50)
	if total != 2 || len(results) != 2 {
		t.Fatalf("prefix q=25 got %d results (total %d), want 2", len(results), total)
	}
	for _, res := range results {
		if res.PubKey == "ff9901" {
			t.Errorf("name-only match ff9901 leaked into prefix results")
		}
	}
}

// TestHandleSearchCaretPrefix exercises the full HTTP path: a q=^25 request must
// return only pubkey-prefix hits, not the name match.
func TestHandleSearchCaretPrefix(t *testing.T) {
	r := newNodeRegistry(defaultAdvertsPerNode)
	r.Observe(AdvertObservation{PubKey: "25aa01", Name: "Alpha", At: 100, NetworkID: "net"})
	r.Observe(AdvertObservation{PubKey: "ff9901", Name: "Room 25", At: 300, NetworkID: "net"})
	s := &Server{nodes: r}

	req := httptest.NewRequest(http.MethodGet, "/api/search?q=%5E25", nil) // %5E = "^"
	rr := httptest.NewRecorder()
	s.handleSearch(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}

	var body struct {
		Results []SearchResult `json:"results"`
		Total   int            `json:"total"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Total != 1 || len(body.Results) != 1 || body.Results[0].PubKey != "25aa01" {
		t.Fatalf("q=^25 got %+v (total %d), want only 25aa01", body.Results, body.Total)
	}
}
