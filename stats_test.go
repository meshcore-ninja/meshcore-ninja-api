package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestStatsAPIAndMetrics(t *testing.T) {
	nodes := newNodeRegistry(defaultAdvertsPerNode)
	nodes.Observe(AdvertObservation{PubKey: "aa", At: 100})
	nodes.Observe(AdvertObservation{PubKey: "bb", At: 200})

	imported := newImportRegistry()
	imported.Replace([]*ImportedNode{
		{PublicKey: "aa"},
		{PublicKey: "cc"},
		{PublicKey: "dd"},
	})

	dir := t.TempDir()
	db, err := OpenDB(filepath.Join(dir, "core.db"), filepath.Join(dir, "links.db"))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()
	if err := db.SaveNodes([]NodeRecord{{PubKey: "aa"}}, 300); err != nil {
		t.Fatalf("SaveNodes: %v", err)
	}
	db.setLoadedNodeCount(1)
	if err := db.AppendAdverts([]AdvertObservation{{PubKey: "aa", At: 100}, {PubKey: "bb", At: 200}}); err != nil {
		t.Fatalf("AppendAdverts: %v", err)
	}
	if _, err := db.db.Exec(`INSERT INTO imported_nodes (public_key) VALUES ('aa'), ('cc'), ('dd')`); err != nil {
		t.Fatalf("insert imported nodes: %v", err)
	}
	if err := db.refreshImportedNodeCount(); err != nil {
		t.Fatalf("refresh imported node count: %v", err)
	}
	if _, err := db.db.Exec(`INSERT INTO imported_node_history (public_key, sig) VALUES ('aa', 's1'), ('cc', 's2')`); err != nil {
		t.Fatalf("insert imported history: %v", err)
	}
	if err := db.refreshImportedNodeHistoryCount(); err != nil {
		t.Fatalf("refresh imported history count: %v", err)
	}

	metrics := NewMetrics()
	srv := NewServer(NewStore(nil), nodes, newObserverRegistry(), newLinkRegistry(defaultLinkHalfLife), imported, db, metrics, nil, "*")

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("stats status = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	var stats statsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if stats.Nodes.Live != 2 || stats.Nodes.Imported != 3 || stats.Nodes.Total != 5 {
		t.Fatalf("node stats = %+v, want live=2 imported=3 total=5", stats.Nodes)
	}
	if stats.Directory.Total != 4 {
		t.Fatalf("directory total = %d, want 4 merged unique nodes", stats.Directory.Total)
	}
	if stats.Directory.Sources.Advert != 2 || stats.Directory.Sources.Map != 3 || stats.Directory.Sources.CoreScope != 2 {
		t.Fatalf("directory sources = %+v, want advert=2 map=3 corescope=2", stats.Directory.Sources)
	}
	if stats.Directory.Types.Unknown != 4 {
		t.Fatalf("directory types = %+v, want 4 unknown", stats.Directory.Types)
	}
	if stats.Directory.Freshness.OlderThan30d != 4 {
		t.Fatalf("directory freshness = %+v, want all older than 30d", stats.Directory.Freshness)
	}
	if stats.SQLite == nil || stats.SQLite.Nodes != 1 || stats.SQLite.ImportedNodes != 3 || stats.SQLite.Adverts != 2 || stats.SQLite.ImportedNodeHistory != 2 {
		t.Fatalf("sqlite stats = %+v, want nodes=1 imported=3 adverts=2 imported_history=2", stats.SQLite)
	}

	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	for _, want := range []string{
		`meshcore_registry_nodes_current{source="live"} 2`,
		`meshcore_registry_nodes_current{source="imported"} 3`,
		`meshcore_sqlite_rows{table="nodes"} 1`,
		`meshcore_sqlite_rows{table="imported_nodes"} 3`,
		`meshcore_sqlite_rows{table="adverts"} 2`,
		`meshcore_sqlite_rows{table="imported_node_history"} 2`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q in:\n%s", want, body)
		}
	}
}
