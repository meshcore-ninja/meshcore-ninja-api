package main

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func TestSaveLoadNodes(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenDB(filepath.Join(dir, "core.db"), filepath.Join(dir, "links.db"))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	nodes := []NodeRecord{{
		PubKey: "aa", Name: "Repeater", NodeType: 2, HasGPS: true,
		Lat: 50.1, Lon: 14.4, FirstAdvertAt: 100, LastAdvertAt: 200,
		AdvertCount: 5, Networks: []string{"net1", "net2"},
		ObserverID: "o1", ObserverName: "Obs",
	}}
	if err := db.SaveNodes(nodes, 300); err != nil {
		t.Fatalf("SaveNodes: %v", err)
	}

	got, err := db.LoadNodes()
	if err != nil {
		t.Fatalf("LoadNodes: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("nodes = %d, want 1", len(got))
	}
	n := got[0]
	if n.PubKey != "aa" || !n.HasGPS || n.AdvertCount != 5 {
		t.Errorf("scalar round-trip = %+v", n)
	}
	if len(n.Networks) != 2 || n.Networks[0] != "net1" || n.Networks[1] != "net2" {
		t.Errorf("networks round-trip = %v", n.Networks)
	}

	// Upsert: a second save updates the same row rather than duplicating it.
	nodes[0].Name = "Repeater v2"
	nodes[0].AdvertCount = 6
	if err := db.SaveNodes(nodes, 400); err != nil {
		t.Fatalf("SaveNodes 2: %v", err)
	}
	got, _ = db.LoadNodes()
	if len(got) != 1 || got[0].Name != "Repeater v2" || got[0].AdvertCount != 6 {
		t.Errorf("after upsert: %+v, want one row name=Repeater v2 count=6", got)
	}
}

func TestOpenDBDropsLegacyLastHashColumn(t *testing.T) {
	dir := t.TempDir()
	corePath := filepath.Join(dir, "core.db")
	linksPath := filepath.Join(dir, "links.db")

	legacy, err := sql.Open("sqlite", "file:"+linksPath)
	if err != nil {
		t.Fatalf("open legacy links db: %v", err)
	}
	if _, err := legacy.Exec(linkSchema); err != nil {
		t.Fatalf("legacy link schema: %v", err)
	}
	if err := ensureColumn(legacy, "links", "last_hash", "TEXT NOT NULL DEFAULT ''"); err != nil {
		t.Fatalf("add legacy last_hash: %v", err)
	}
	if ok, err := columnExists(legacy, "links", "last_hash"); err != nil || !ok {
		t.Fatalf("legacy last_hash exists = %v, err=%v; want true/nil", ok, err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy links db: %v", err)
	}

	db, err := OpenDB(corePath, linksPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	if ok, err := columnExists(db.links, "links", "last_hash"); err != nil || ok {
		t.Fatalf("last_hash exists after migration = %v, err=%v; want false/nil", ok, err)
	}
}

func TestAppendLoadRecentAdverts(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenDB(filepath.Join(dir, "core.db"), filepath.Join(dir, "links.db"))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	// Two nodes; node "aa" gets more adverts than the per-node cap we'll request.
	var batch []AdvertObservation
	for i := 0; i < 4; i++ {
		batch = append(batch, AdvertObservation{PubKey: "aa", At: int64(100 + i), NetworkID: "net1"})
	}
	batch = append(batch, AdvertObservation{PubKey: "bb", At: 500, NetworkID: "net2"})
	if err := db.AppendAdverts(batch); err != nil {
		t.Fatalf("AppendAdverts: %v", err)
	}
	// A second batch appended later must keep id ordering (newest first on load).
	if err := db.AppendAdverts([]AdvertObservation{{PubKey: "aa", At: 999, NetworkID: "net1"}}); err != nil {
		t.Fatalf("AppendAdverts 2: %v", err)
	}

	recent, err := db.LoadRecentAdverts([]string{"aa", "bb"}, 3)
	if err != nil {
		t.Fatalf("LoadRecentAdverts: %v", err)
	}
	if len(recent["aa"]) != 3 {
		t.Fatalf("aa recent = %d, want 3 (capped)", len(recent["aa"]))
	}
	// Newest first: the last-appended (At=999) leads, then At=103, 102.
	wantAt := []int64{999, 103, 102}
	for i, w := range wantAt {
		if recent["aa"][i].At != w {
			t.Errorf("aa recent[%d].at = %d, want %d", i, recent["aa"][i].At, w)
		}
	}
	if len(recent["bb"]) != 1 || recent["bb"][0].At != 500 {
		t.Errorf("bb recent = %+v, want one entry at 500", recent["bb"])
	}

	recent, err = db.LoadRecentAdverts([]string{"bb"}, 3)
	if err != nil {
		t.Fatalf("LoadRecentAdverts targeted: %v", err)
	}
	if _, ok := recent["aa"]; ok {
		t.Errorf("targeted recent included aa: %+v", recent["aa"])
	}
}
