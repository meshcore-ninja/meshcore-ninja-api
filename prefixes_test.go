package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func prefixTestServer() *Server {
	r := newNodeRegistry(defaultAdvertsPerNode)
	// Two nodes share the 1-byte prefix "25"; a third has "ff".
	r.Observe(AdvertObservation{PubKey: "25aa01", Name: "Alpha", At: 100, NetworkID: "net"})
	r.Observe(AdvertObservation{PubKey: "25bb02", Name: "Beta", At: 200, NetworkID: "net"})
	r.Observe(AdvertObservation{PubKey: "ff9901", Name: "Gamma", At: 300, NetworkID: "net"})
	return &Server{nodes: r}
}

type prefixResp struct {
	Bytes      int `json:"bytes"`
	Counted    int `json:"counted"`
	Space      int `json:"space"`
	Used       int `json:"used"`
	Collisions int `json:"collisions"`
	Prefixes   []struct {
		Prefix string `json:"prefix"`
		Count  int    `json:"count"`
		Nodes  []struct {
			PubKey string `json:"pubkey"`
			Name   string `json:"name"`
		} `json:"nodes"`
	} `json:"prefixes"`
}

// TestHandlePrefixesOneByte checks the 1-byte histogram groups the two "25"
// nodes together and reports one collision.
func TestHandlePrefixesOneByte(t *testing.T) {
	s := prefixTestServer()
	req := httptest.NewRequest(http.MethodGet, "/api/prefixes?networks=net&bytes=1", nil)
	rr := httptest.NewRecorder()
	s.handlePrefixes(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var body prefixResp
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Space != 256 || body.Counted != 3 || body.Used != 2 || body.Collisions != 1 {
		t.Fatalf("got space=%d counted=%d used=%d collisions=%d, want 256/3/2/1",
			body.Space, body.Counted, body.Used, body.Collisions)
	}
	// Most-crowded first: the "25" bucket with 2 nodes leads.
	if len(body.Prefixes) != 2 || body.Prefixes[0].Prefix != "25" || body.Prefixes[0].Count != 2 {
		t.Fatalf("unexpected prefixes: %+v", body.Prefixes)
	}
	if len(body.Prefixes[0].Nodes) != 2 {
		t.Fatalf("prefix 25 should carry 2 nodes, got %d", len(body.Prefixes[0].Nodes))
	}
}

// TestHandlePrefixesTwoBytes widens the prefix so all three keys become distinct.
func TestHandlePrefixesTwoBytes(t *testing.T) {
	s := prefixTestServer()
	req := httptest.NewRequest(http.MethodGet, "/api/prefixes?networks=net&bytes=2", nil)
	rr := httptest.NewRecorder()
	s.handlePrefixes(rr, req)
	var body prefixResp
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Space != 65536 || body.Used != 3 || body.Collisions != 0 {
		t.Fatalf("got space=%d used=%d collisions=%d, want 65536/3/0",
			body.Space, body.Used, body.Collisions)
	}
}

// TestHandlePrefixesRequiresNetwork rejects a request that omits networks.
func TestHandlePrefixesRequiresNetwork(t *testing.T) {
	s := prefixTestServer()
	req := httptest.NewRequest(http.MethodGet, "/api/prefixes?bytes=1", nil)
	rr := httptest.NewRecorder()
	s.handlePrefixes(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}
