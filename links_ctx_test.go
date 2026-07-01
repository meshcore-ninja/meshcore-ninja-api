package main

import (
	"path/filepath"
	"testing"
)

// TestLinkDirectionAndSources checks that path order yields directional counts
// (relative to the queried node) and that route types are tallied per link.
func TestLinkDirectionAndSources(t *testing.T) {
	a, b := pk(1), pk(2) // pk(1) < pk(2): canonical NodeA=a, NodeB=b
	reg := noDecay()

	reg.ObservePathCtx(PathObservation{Hash: "h1", NetworkID: "net", RouteType: "flood", Path: []string{a, b}, Now: 100})
	reg.ObservePathCtx(PathObservation{Hash: "h2", NetworkID: "net", RouteType: "direct", Path: []string{b, a}, Now: 101})

	la := mustNeighbor(t, reg, a, b)
	if la.SentByNode != 1 || la.RecvByNode != 1 {
		t.Errorf("from A: sent=%d recv=%d, want 1/1", la.SentByNode, la.RecvByNode)
	}
	if la.PacketCount != 2 {
		t.Errorf("packetCount = %d, want 2", la.PacketCount)
	}
	if la.LastHash != "h1" {
		t.Errorf("from A lastHash = %q, want h1", la.LastHash)
	}
	if la.Sources["flood"] != 1 || la.Sources["direct"] != 1 {
		t.Errorf("sources = %v, want flood:1 direct:1", la.Sources)
	}

	// From B's perspective the direction is mirrored.
	lb := mustNeighbor(t, reg, b, a)
	if lb.SentByNode != 1 || lb.RecvByNode != 1 {
		t.Errorf("from B: sent=%d recv=%d, want 1/1", lb.SentByNode, lb.RecvByNode)
	}
	if lb.LastHash != "h2" {
		t.Errorf("from B lastHash = %q, want h2", lb.LastHash)
	}
}

// TestLinkOneWayDirection confirms an asymmetric link (only ever A→B) is visible.
func TestLinkOneWayDirection(t *testing.T) {
	a, b := pk(1), pk(2)
	reg := noDecay()
	reg.ObservePathCtx(PathObservation{Hash: "h1", Path: []string{a, b}, RouteType: "flood", Now: 100})
	reg.ObservePathCtx(PathObservation{Hash: "h2", Path: []string{a, b}, RouteType: "flood", Now: 101})

	l := mustNeighbor(t, reg, a, b)
	if l.SentByNode != 2 || l.RecvByNode != 0 {
		t.Errorf("one-way A→B: sent=%d recv=%d, want 2/0", l.SentByNode, l.RecvByNode)
	}
	if l.Sources["unknown"] != 0 {
		t.Errorf("did not expect unknown-source events: %v", l.Sources)
	}
}

// TestLinkUnknownRouteType checks the bare ObservePath wrapper tags events as
// route type "unknown".
func TestLinkUnknownRouteType(t *testing.T) {
	a, b := pk(1), pk(2)
	reg := noDecay()
	reg.ObservePath("h1", "net", []string{a, b}, 100)
	if l := mustNeighbor(t, reg, a, b); l.Sources["unknown"] != 1 {
		t.Errorf("sources = %v, want unknown:1", l.Sources)
	}
}

// TestLinkSNRAttribution verifies TRACE SNRs land on the right links, in
// accepted-hop order.
func TestLinkSNRAttribution(t *testing.T) {
	a, b, c := pk(1), pk(2), pk(3)
	reg := noDecay()
	reg.ObservePathCtx(PathObservation{
		Hash: "h1", Path: []string{a, b, c}, RouteType: "direct",
		SNRs: []float64{7.25, -3.0}, Now: 100,
	})

	ab := mustNeighbor(t, reg, a, b)
	if !ab.HasSNR || ab.LastSNR != 7.25 {
		t.Errorf("A—B snr: has=%v val=%.2f, want true/7.25", ab.HasSNR, ab.LastSNR)
	}
	bc := mustNeighbor(t, reg, b, c)
	if !bc.HasSNR || bc.LastSNR != -3.0 {
		t.Errorf("B—C snr: has=%v val=%.2f, want true/-3.00", bc.HasSNR, bc.LastSNR)
	}
}

func TestLinkSNRHistoryIsDirectionalAndCapped(t *testing.T) {
	a, b := pk(1), pk(2) // pk(1) < pk(2): canonical NodeA=a, NodeB=b
	reg := noDecay()

	for i := 1; i <= 6; i++ {
		reg.ObservePathCtx(PathObservation{
			Hash: string(rune('a' + i)), Path: []string{a, b}, RouteType: "direct",
			SNRs: []float64{float64(i)}, Now: int64(100 + i),
		})
	}
	reg.ObservePathCtx(PathObservation{Hash: "r1", Path: []string{b, a}, SNRs: []float64{-1.5}, Now: 200})
	reg.ObservePathCtx(PathObservation{Hash: "r2", Path: []string{b, a}, SNRs: []float64{-2.5}, Now: 201})

	fromA := mustNeighbor(t, reg, a, b)
	wantA := []float64{2, 3, 4, 5, 6}
	if !equalFloatSlices(fromA.SNRs, wantA) || !fromA.HasSNR || fromA.LastSNR != 6 {
		t.Errorf("A->B snrs=%v has=%v last=%.2f, want %v true/6", fromA.SNRs, fromA.HasSNR, fromA.LastSNR, wantA)
	}

	fromB := mustNeighbor(t, reg, b, a)
	wantB := []float64{-1.5, -2.5}
	if !equalFloatSlices(fromB.SNRs, wantB) || !fromB.HasSNR || fromB.LastSNR != -2.5 {
		t.Errorf("B->A snrs=%v has=%v last=%.2f, want %v true/-2.5", fromB.SNRs, fromB.HasSNR, fromB.LastSNR, wantB)
	}
}

// TestLinkPositionSegments checks positions are stamped from PosOf and that a
// move beyond the epsilon freezes a historical segment.
func TestLinkPositionSegments(t *testing.T) {
	a, b := pk(1), pk(2)
	pos := map[string][2]float64{
		a: {50.0, 14.0},
		b: {50.1, 14.1},
	}
	posOf := func(k string) (float64, float64, bool) {
		p, ok := pos[k]
		if !ok {
			return 0, 0, false
		}
		return p[0], p[1], true
	}
	reg := noDecay()
	reg.ObservePathCtx(PathObservation{Hash: "h1", Path: []string{a, b}, Now: 100, PosOf: posOf})

	l := mustNeighbor(t, reg, a, b)
	if !l.HasPos || l.Moved || l.SegmentCount != 0 {
		t.Fatalf("first obs: hasPos=%v moved=%v segs=%d, want true/false/0", l.HasPos, l.Moved, l.SegmentCount)
	}
	if l.NeighborLat != 50.1 || l.NeighborLon != 14.1 {
		t.Errorf("neighbor pos = %.2f,%.2f, want 50.10,14.10", l.NeighborLat, l.NeighborLon)
	}

	// B moves far away; a new counted packet should freeze the old segment.
	pos[b] = [2]float64{60.0, 20.0}
	reg.ObservePathCtx(PathObservation{Hash: "h2", Path: []string{a, b}, Now: 200, PosOf: posOf})

	l = mustNeighbor(t, reg, a, b)
	if !l.Moved || l.SegmentCount != 1 {
		t.Errorf("after move: moved=%v segs=%d, want true/1", l.Moved, l.SegmentCount)
	}
	if l.NeighborLat != 60.0 {
		t.Errorf("neighbor now at %.2f, want 60.00", l.NeighborLat)
	}

	// GPS jitter (< epsilon) must NOT open a new segment.
	pos[b] = [2]float64{60.001, 20.001}
	reg.ObservePathCtx(PathObservation{Hash: "h3", Path: []string{a, b}, Now: 300, PosOf: posOf})
	if l := mustNeighbor(t, reg, a, b); l.SegmentCount != 1 {
		t.Errorf("jitter opened a segment: segs=%d, want 1", l.SegmentCount)
	}
}

// TestLinkPersistenceRoundTrip saves an enriched link and reloads it, confirming
// direction, hash, SNR, sources, and position segments survive.
func TestLinkPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenDB(filepath.Join(dir, "core.db"), filepath.Join(dir, "links.db"))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	a, b := pk(1), pk(2)
	pos := map[string][2]float64{a: {50, 14}, b: {50.1, 14.1}}
	posOf := func(k string) (float64, float64, bool) { p, ok := pos[k]; return p[0], p[1], ok }

	src := noDecay()
	src.ObservePathCtx(PathObservation{Hash: "h1", NetworkID: "net", RouteType: "flood", Path: []string{a, b}, SNRs: []float64{9.5}, Now: 100, PosOf: posOf})
	pos[b] = [2]float64{60, 20}
	src.ObservePathCtx(PathObservation{Hash: "h2", NetworkID: "net", RouteType: "direct", Path: []string{b, a}, SNRs: []float64{-4.5}, Now: 200, PosOf: posOf})

	dirty := src.TakeDirty()
	if err := db.SaveLinks(dirty, 200); err != nil {
		t.Fatalf("SaveLinks: %v", err)
	}

	loaded, err := db.LoadLinks()
	if err != nil {
		t.Fatalf("LoadLinks: %v", err)
	}
	dst := noDecay()
	dst.Restore(loaded)

	l := mustNeighbor(t, dst, a, b)
	if l.PacketCount != 2 {
		t.Errorf("packetCount = %d, want 2", l.PacketCount)
	}
	if l.SentByNode != 1 || l.RecvByNode != 1 {
		t.Errorf("direction sent=%d recv=%d, want 1/1", l.SentByNode, l.RecvByNode)
	}
	if l.Sources["flood"] != 1 || l.Sources["direct"] != 1 {
		t.Errorf("sources = %v, want flood:1 direct:1", l.Sources)
	}
	if l.LastHash != "h1" {
		t.Errorf("A->B lastHash = %q, want h1", l.LastHash)
	}
	if !l.HasSNR || l.LastSNR != 9.5 || !equalFloatSlices(l.SNRs, []float64{9.5}) {
		t.Errorf("A->B snr has=%v val=%.2f history=%v, want true/9.50/[9.5]", l.HasSNR, l.LastSNR, l.SNRs)
	}
	if rev := mustNeighbor(t, dst, b, a); rev.LastHash != "h2" || !rev.HasSNR || rev.LastSNR != -4.5 || !equalFloatSlices(rev.SNRs, []float64{-4.5}) {
		t.Errorf("B->A hash=%q snr has=%v val=%.2f history=%v, want h2 true/-4.50/[-4.5]", rev.LastHash, rev.HasSNR, rev.LastSNR, rev.SNRs)
	}
	if !l.Moved || l.SegmentCount != 1 {
		t.Errorf("geometry moved=%v segs=%d, want true/1", l.Moved, l.SegmentCount)
	}
}

func equalFloatSlices(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
