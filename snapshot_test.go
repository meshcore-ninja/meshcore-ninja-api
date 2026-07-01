package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newTestSnapshotter returns a snapshotter wired to the standard test registry
// and an empty import registry, writing into a fresh temp directory.
func newTestSnapshotter(t *testing.T) (*MapSnapshotter, string) {
	t.Helper()
	dir := t.TempDir()
	r := newTestRegistry()
	imp := newImportRegistry()
	return NewMapSnapshotter(r, imp, dir, "https://example.com"), dir
}

// TestSnapshotAtomicPublication verifies that no .tmp file lingers after a
// successful generateOnce call, and that exactly one .json.zst file exists.
func TestSnapshotAtomicPublication(t *testing.T) {
	s, dir := newTestSnapshotter(t)
	if err := s.generateOnce(); err != nil {
		t.Fatalf("generateOnce: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var zsts, tmps int
	for _, e := range entries {
		switch {
		case strings.HasSuffix(e.Name(), ".json.zst"):
			zsts++
		case strings.HasSuffix(e.Name(), ".tmp"):
			tmps++
		}
	}
	if tmps != 0 {
		t.Errorf("expected no .tmp files after publication, found %d", tmps)
	}
	if zsts != 1 {
		t.Errorf("expected exactly 1 .json.zst file, found %d", zsts)
	}
}

// TestSnapshotExcludesLocationFlagged verifies that nodes with an untrustworthy
// location — whether flagged far_from_network or network_too_far — are omitted
// from the snapshot node set, while their clean peers remain.
func TestSnapshotExcludesLocationFlagged(t *testing.T) {
	s, _ := newTestSnapshotter(t)
	s.nodes.ApplyFlags(map[string][]string{
		"aa01": {FlagFarFromNetwork},
		"aa02": {FlagNetworkTooFar},
	})

	nodes := s.collectNodes()
	for _, tuple := range nodes {
		if tuple[0] == "aa01" || tuple[0] == "aa02" {
			t.Fatalf("location-flagged node %v should be excluded from the snapshot", tuple[0])
		}
	}
	// The other seeded GPS nodes must still be present.
	for _, want := range []string{"aa03", "bb01"} {
		found := false
		for _, tuple := range nodes {
			if tuple[0] == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected unflagged node %s in snapshot", want)
		}
	}
}

// TestSnapshotManifestCorrectness checks that latest.json contains all required
// fields with coherent values after a generateOnce.
func TestSnapshotManifestCorrectness(t *testing.T) {
	s, dir := newTestSnapshotter(t)
	before := time.Now().UTC().Truncate(time.Second)
	if err := s.generateOnce(); err != nil {
		t.Fatalf("generateOnce: %v", err)
	}
	after := time.Now().UTC().Add(time.Second)

	raw, err := os.ReadFile(filepath.Join(dir, "latest.json"))
	if err != nil {
		t.Fatalf("reading latest.json: %v", err)
	}
	var m SnapshotManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("parsing latest.json: %v", err)
	}

	if m.FormatVersion != snapshotFormatVersion {
		t.Errorf("formatVersion = %d, want %d", m.FormatVersion, snapshotFormatVersion)
	}
	if m.URL == "" {
		t.Error("url is empty")
	}
	if !strings.Contains(m.URL, "/api/snapshots/map-") || !strings.HasSuffix(m.URL, ".json") {
		t.Errorf("unexpected url shape: %q", m.URL)
	}
	if m.NodeCount <= 0 {
		t.Errorf("nodeCount = %d, want > 0", m.NodeCount)
	}
	if m.CompressedSize <= 0 {
		t.Errorf("compressedSize = %d, want > 0", m.CompressedSize)
	}
	genAt, err := time.Parse(time.RFC3339, m.GeneratedAt)
	if err != nil {
		t.Fatalf("generatedAt not RFC3339: %q", m.GeneratedAt)
	}
	if genAt.Before(before) || genAt.After(after) {
		t.Errorf("generatedAt %v outside [%v, %v]", genAt, before, after)
	}

	// In-memory manifest must match the on-disk one.
	mem := s.CurrentManifest()
	if mem == nil {
		t.Fatal("CurrentManifest() is nil")
	}
	if mem.URL != m.URL || mem.NodeCount != m.NodeCount || mem.CompressedSize != m.CompressedSize {
		t.Errorf("in-memory manifest %+v differs from disk %+v", mem, m)
	}
}

// TestSnapshotZstdResponseHeaders verifies the snapshot HTTP handler sets the
// correct content type, encoding, and cache headers for a zstd-capable client.
func TestSnapshotZstdResponseHeaders(t *testing.T) {
	s, _ := newTestSnapshotter(t)
	if err := s.generateOnce(); err != nil {
		t.Fatalf("generateOnce: %v", err)
	}

	m := s.CurrentManifest()
	// Extract just the path portion from the logical URL.
	urlPath := m.URL[strings.Index(m.URL, "/api/snapshots/"):]

	req := httptest.NewRequest(http.MethodGet, urlPath, nil)
	req.Header.Set("Accept-Encoding", "zstd")
	rw := httptest.NewRecorder()
	s.ServeSnapshot(rw, req)

	res := rw.Result()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if ce := res.Header.Get("Content-Encoding"); ce != "zstd" {
		t.Errorf("Content-Encoding = %q, want zstd", ce)
	}
	if cc := res.Header.Get("Cache-Control"); cc != "public, max-age=31536000, immutable" {
		t.Errorf("Cache-Control = %q", cc)
	}
	if rw.Body.Len() == 0 {
		t.Error("response body is empty")
	}
}

// TestSnapshotUnsupportedClient verifies the handler returns 406 when the
// client does not advertise zstd support.
func TestSnapshotUnsupportedClient(t *testing.T) {
	s, _ := newTestSnapshotter(t)
	if err := s.generateOnce(); err != nil {
		t.Fatalf("generateOnce: %v", err)
	}

	m := s.CurrentManifest()
	urlPath := m.URL[strings.Index(m.URL, "/api/snapshots/"):]

	for _, ae := range []string{"", "gzip", "br, gzip"} {
		req := httptest.NewRequest(http.MethodGet, urlPath, nil)
		if ae != "" {
			req.Header.Set("Accept-Encoding", ae)
		}
		rw := httptest.NewRecorder()
		s.ServeSnapshot(rw, req)
		if rw.Code != http.StatusNotAcceptable {
			t.Errorf("Accept-Encoding %q: status = %d, want 406", ae, rw.Code)
		}
	}
}

// TestSnapshotRetentionTwoFiles asserts that after three generateOnce calls
// only two .json.zst files remain (current + previous).
func TestSnapshotRetentionTwoFiles(t *testing.T) {
	s, dir := newTestSnapshotter(t)
	for i := 0; i < 3; i++ {
		// Sleep 1 second so each snapshot gets a distinct timestamp filename.
		if i > 0 {
			time.Sleep(time.Second)
		}
		if err := s.generateOnce(); err != nil {
			t.Fatalf("generateOnce #%d: %v", i+1, err)
		}
	}
	entries, _ := os.ReadDir(dir)
	var zsts []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "map-") && strings.HasSuffix(e.Name(), ".json.zst") {
			zsts = append(zsts, e.Name())
		}
	}
	if len(zsts) != 2 {
		t.Errorf("expected 2 snapshot files after 3 generations, found %d: %v", len(zsts), zsts)
	}
}

// TestSnapshotStartupAndPeriodicGeneration verifies that Run blocks until
// startupDone fires, generates an initial snapshot, and then regenerates on the
// configured interval.
func TestSnapshotStartupAndPeriodicGeneration(t *testing.T) {
	s, dir := newTestSnapshotter(t)
	startupDone := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const interval = 100 * time.Millisecond
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Run(ctx, startupDone, interval)
	}()

	// No snapshot yet — startupDone is still open.
	time.Sleep(20 * time.Millisecond)
	if s.CurrentManifest() != nil {
		t.Error("snapshot generated before startupDone was closed")
	}

	close(startupDone)

	// Wait for the startup snapshot.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.CurrentManifest() != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if s.CurrentManifest() == nil {
		t.Fatal("no snapshot generated after startupDone was closed")
	}
	firstURL := s.CurrentManifest().URL

	// Wait for a second snapshot on the interval ticker.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cur := s.CurrentManifest(); cur != nil && cur.URL != firstURL {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if cur := s.CurrentManifest(); cur == nil || cur.URL == firstURL {
		t.Error("periodic snapshot not generated within 2 seconds")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("Run did not return after context cancellation")
	}

	// Verify at least one snapshot file is on disk.
	entries, _ := os.ReadDir(dir)
	var zsts int
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "map-") && strings.HasSuffix(e.Name(), ".json.zst") {
			zsts++
		}
	}
	if zsts == 0 {
		t.Error("no snapshot files found on disk")
	}
}

// TestSnapshotManifestRoute verifies ServeManifest delivers latest.json with
// the expected headers.
func TestSnapshotManifestRoute(t *testing.T) {
	s, _ := newTestSnapshotter(t)
	if err := s.generateOnce(); err != nil {
		t.Fatalf("generateOnce: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/snapshots/latest.json", nil)
	rw := httptest.NewRecorder()
	s.ServeManifest(rw, req)

	res := rw.Result()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if cc := res.Header.Get("Cache-Control"); cc != "public, max-age=30" {
		t.Errorf("Cache-Control = %q, want public, max-age=30", cc)
	}
	var m SnapshotManifest
	if err := json.NewDecoder(rw.Body).Decode(&m); err != nil {
		t.Fatalf("parsing manifest response: %v", err)
	}
	if m.FormatVersion != snapshotFormatVersion {
		t.Errorf("formatVersion = %d", m.FormatVersion)
	}
}
