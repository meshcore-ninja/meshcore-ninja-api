package main

import (
	"path/filepath"
	"testing"
)

// TestObserverRegistryCanonicalizesID guards the isObserver bug: analyzers report
// observer ids in upper case, but node pubkeys are lower-cased everywhere, so a
// self-observing node must still be found by its lower-case pubkey.
func TestObserverRegistryCanonicalizesID(t *testing.T) {
	reg := newObserverRegistry()
	reg.Observe(ObserverActivity{ObserverID: "A5A5A5A2FFF0715D", Name: "Owl", NetworkID: "net", At: 100})

	if _, ok := reg.Lookup("a5a5a5a2fff0715d"); !ok {
		t.Fatal("Lookup by lower-case pubkey failed; observer keyed under upper-case id")
	}
	if snap := reg.Snapshot(); len(snap) != 1 || snap[0].ObserverID != "a5a5a5a2fff0715d" {
		t.Fatalf("snapshot = %+v, want single lower-cased observer id", snap)
	}

	// A second report with different casing must fold into the same row.
	reg.Observe(ObserverActivity{ObserverID: "a5a5a5a2fff0715d", NetworkID: "net", At: 200})
	if snap := reg.Snapshot(); len(snap) != 1 || snap[0].Observations != 2 {
		t.Fatalf("snapshot = %+v, want one row with 2 observations", snap)
	}
}

// TestObserverRegistryRestoreMergesLegacyCasing covers persisted rows that predate
// canonicalization: an upper-cased row and its lower-cased twin must merge.
func TestObserverRegistryRestoreMergesLegacyCasing(t *testing.T) {
	reg := newObserverRegistry()
	reg.Restore([]ObserverRecord{
		{ObserverID: "ABCD", Name: "Legacy", FirstSeen: 100, LastSeen: 150, Observations: 3, Networks: []string{"net-a"}},
		{ObserverID: "abcd", FirstSeen: 90, LastSeen: 200, Observations: 2, Networks: []string{"net-b"}},
	})
	snap := reg.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("observers = %d, want 1 (merged)", len(snap))
	}
	o := snap[0]
	if o.ObserverID != "abcd" || o.FirstSeen != 90 || o.LastSeen != 200 || o.Observations != 5 {
		t.Fatalf("merged = %+v, want abcd first=90 last=200 obs=5", o)
	}
	if len(o.Networks) != 2 {
		t.Errorf("networks = %v, want union of both rows", o.Networks)
	}
}

func TestObserverRegistryObserve(t *testing.T) {
	reg := newObserverRegistry()

	// Same observer seen twice across two networks: one row, count advances,
	// network set grows, last-seen tracks the newest.
	reg.Observe(ObserverActivity{ObserverID: "o1", Name: "Alpha", NetworkID: "net-a", At: 100})
	reg.Observe(ObserverActivity{ObserverID: "o1", NetworkID: "net-b", At: 200})
	reg.Observe(ObserverActivity{ObserverID: "o2", Name: "Beta", NetworkID: "net-a", At: 150})

	// Empty observer id is ignored.
	reg.Observe(ObserverActivity{ObserverID: "", At: 999})

	obs := reg.Snapshot()
	if len(obs) != 2 {
		t.Fatalf("observers = %d, want 2", len(obs))
	}
	// Sorted by last-seen desc: o1 (200) then o2 (150).
	if obs[0].ObserverID != "o1" || obs[1].ObserverID != "o2" {
		t.Fatalf("order = %q,%q, want o1,o2", obs[0].ObserverID, obs[1].ObserverID)
	}
	o1 := obs[0]
	if o1.Name != "Alpha" {
		t.Errorf("name = %q, want Alpha (kept across empty-name report)", o1.Name)
	}
	if o1.FirstSeen != 100 || o1.LastSeen != 200 {
		t.Errorf("seen = %d/%d, want 100/200", o1.FirstSeen, o1.LastSeen)
	}
	if o1.Observations != 2 {
		t.Errorf("observations = %d, want 2", o1.Observations)
	}
	if len(o1.Networks) != 2 || o1.Networks[0] != "net-a" || o1.Networks[1] != "net-b" {
		t.Errorf("networks = %v, want [net-a net-b]", o1.Networks)
	}
}

func TestSaveLoadObservers(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenDB(filepath.Join(dir, "core.db"), filepath.Join(dir, "links.db"))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	observers := []ObserverRecord{{
		ObserverID: "o1", Name: "Alpha", FirstSeen: 100, LastSeen: 200,
		Observations: 7, Networks: []string{"net-a", "net-b"},
	}}
	if err := db.SaveObservers(observers, 300); err != nil {
		t.Fatalf("SaveObservers: %v", err)
	}

	got, err := db.LoadObservers()
	if err != nil {
		t.Fatalf("LoadObservers: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("observers = %d, want 1", len(got))
	}
	o := got[0]
	if o.ObserverID != "o1" || o.Name != "Alpha" || o.LastSeen != 200 || o.Observations != 7 {
		t.Errorf("round-trip = %+v", o)
	}
	if len(o.Networks) != 2 || o.Networks[1] != "net-b" {
		t.Errorf("networks round-trip = %v", o.Networks)
	}

	// Upsert: a second save updates the same row rather than duplicating it.
	observers[0].LastSeen = 250
	observers[0].Observations = 9
	if err := db.SaveObservers(observers, 400); err != nil {
		t.Fatalf("SaveObservers 2: %v", err)
	}
	got, _ = db.LoadObservers()
	if len(got) != 1 || got[0].LastSeen != 250 || got[0].Observations != 9 {
		t.Errorf("after upsert: %+v, want one row last_seen=250 obs=9", got)
	}
}

func TestObserverRegistryTakeDirtyAndRequeue(t *testing.T) {
	reg := newObserverRegistry()
	reg.Restore([]ObserverRecord{{
		ObserverID: "restored",
		Name:       "Clean",
		FirstSeen:  10,
		LastSeen:   20,
		Networks:   []string{"net-a"},
	}})
	if dirty := reg.TakeDirty(); len(dirty) != 0 {
		t.Fatalf("restored dirty = %d, want 0", len(dirty))
	}

	reg.Observe(ObserverActivity{ObserverID: "o1", Name: "Alpha", NetworkID: "net-a", At: 100})
	reg.Observe(ObserverActivity{ObserverID: "o1", NetworkID: "net-b", At: 200})
	reg.Observe(ObserverActivity{ObserverID: "o2", Name: "Beta", NetworkID: "net-a", At: 150})

	dirty := reg.TakeDirty()
	if len(dirty) != 2 {
		t.Fatalf("dirty = %d, want 2", len(dirty))
	}
	byID := map[string]ObserverRecord{}
	for _, o := range dirty {
		byID[o.ObserverID] = o
	}
	if byID["o1"].Name != "Alpha" || byID["o1"].Observations != 2 || byID["o1"].LastSeen != 200 {
		t.Errorf("o1 dirty copy = %+v, want latest observer state", byID["o1"])
	}
	if len(byID["o1"].Networks) != 2 {
		t.Errorf("o1 networks = %v, want 2 networks", byID["o1"].Networks)
	}
	if dirty := reg.TakeDirty(); len(dirty) != 0 {
		t.Fatalf("dirty after take = %d, want 0", len(dirty))
	}

	reg.Requeue(dirty)
	if dirty := reg.TakeDirty(); len(dirty) != 2 {
		t.Fatalf("dirty after requeue = %d, want 2", len(dirty))
	}
}
