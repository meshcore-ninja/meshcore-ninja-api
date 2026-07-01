package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

const snapshotFormatVersion = 2

// snapshotPayload is the full-map snapshot written to disk as a zstd-compressed
// JSON file. Nodes is a compact array-of-arrays; each inner tuple is:
// [pubkey, name, nodeType, lat, lon, lastAdvertAt, advertCount, networks[], freqMHz, flags[]].
// advertCount=0 marks imported (map.meshcore.io) nodes that carry no network
// membership; live nodes always have advertCount≥1. freqMHz is the imported
// node's radio frequency (0 for live nodes, which band via their networks).
// flags are the Flagger's quality tags (e.g. "far_from_network"), empty for
// imported nodes which the scan does not evaluate.
type snapshotPayload struct {
	FormatVersion int       `json:"formatVersion"`
	GeneratedAt   string    `json:"generatedAt"`
	Nodes         [][10]any `json:"nodes"`
}

// SnapshotManifest is written to latest.json alongside the snapshot files and
// served with a short cache lifetime so clients discover new snapshots quickly.
type SnapshotManifest struct {
	FormatVersion  int    `json:"formatVersion"`
	GeneratedAt    string `json:"generatedAt"`
	URL            string `json:"url"`
	NodeCount      int    `json:"nodeCount"`
	CompressedSize int64  `json:"compressedSize"`
}

// MapSnapshotter builds versioned full-map snapshots on a ticker and serves
// them over HTTP. Snapshots are zstd-compressed and written atomically; only
// the two most recent files are kept.
type MapSnapshotter struct {
	nodes    *NodeRegistry
	imported *ImportRegistry
	dir      string
	baseURL  string // e.g. "https://meshcore.ninja" — appended with /api/snapshots/...

	mu       sync.Mutex
	manifest *SnapshotManifest
}

// NewMapSnapshotter creates a snapshotter. dir is the on-disk directory for
// snapshot files; baseURL is the public origin prepended to snapshot URLs in
// the manifest (no trailing slash).
func NewMapSnapshotter(nodes *NodeRegistry, imported *ImportRegistry, dir, baseURL string) *MapSnapshotter {
	return &MapSnapshotter{
		nodes:    nodes,
		imported: imported,
		dir:      dir,
		baseURL:  baseURL,
	}
}

// Run waits for startupDone to close (signalling that persisted state has been
// restored), generates an initial snapshot, then regenerates on every interval
// tick until ctx is cancelled.
func (s *MapSnapshotter) Run(ctx context.Context, startupDone <-chan struct{}, interval time.Duration) {
	select {
	case <-ctx.Done():
		return
	case <-startupDone:
	}
	if err := s.generateOnce(); err != nil && ctx.Err() == nil {
		log.Printf("[snapshot] initial generation: %v", err)
	}
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.generateOnce(); err != nil && ctx.Err() == nil {
				log.Printf("[snapshot] generation: %v", err)
			}
		}
	}
}

// generateOnce builds and publishes one snapshot atomically.
func (s *MapSnapshotter) generateOnce() error {
	start := time.Now()
	now := start.UTC()
	ts := now.Format("20060102T150405Z")
	fname := "map-" + ts + ".json.zst"
	logicalURL := strings.TrimRight(s.baseURL, "/") + "/api/snapshots/map-" + ts + ".json"

	tQuery := time.Now()
	nodes := s.collectNodes()
	queryMs := float64(time.Since(tQuery).Microseconds()) / 1000

	tEncode := time.Now()
	payload := snapshotPayload{
		FormatVersion: snapshotFormatVersion,
		GeneratedAt:   now.Format(time.RFC3339),
		Nodes:         nodes,
	}
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling snapshot: %w", err)
	}

	var buf bytes.Buffer
	enc, err := zstd.NewWriter(&buf)
	if err != nil {
		return fmt.Errorf("creating zstd encoder: %w", err)
	}
	if _, err := enc.Write(jsonBytes); err != nil {
		_ = enc.Close()
		return fmt.Errorf("compressing snapshot: %w", err)
	}
	if err := enc.Close(); err != nil {
		return fmt.Errorf("finalizing zstd: %w", err)
	}
	compressed := buf.Bytes()
	encodeMs := float64(time.Since(tEncode).Microseconds()) / 1000

	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return fmt.Errorf("creating snapshot dir: %w", err)
	}

	finalPath := filepath.Join(s.dir, fname)
	tmpPath := finalPath + ".tmp"
	if err := os.WriteFile(tmpPath, compressed, 0o644); err != nil {
		return fmt.Errorf("writing snapshot tmp: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("publishing snapshot: %w", err)
	}

	manifest := &SnapshotManifest{
		FormatVersion:  snapshotFormatVersion,
		GeneratedAt:    now.Format(time.RFC3339),
		URL:            logicalURL,
		NodeCount:      len(nodes),
		CompressedSize: int64(len(compressed)),
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshaling manifest: %w", err)
	}
	manifestPath := filepath.Join(s.dir, "latest.json")
	manifestTmp := manifestPath + ".tmp"
	if err := os.WriteFile(manifestTmp, manifestBytes, 0o644); err != nil {
		return fmt.Errorf("writing manifest tmp: %w", err)
	}
	if err := os.Rename(manifestTmp, manifestPath); err != nil {
		_ = os.Remove(manifestTmp)
		return fmt.Errorf("publishing manifest: %w", err)
	}

	s.mu.Lock()
	s.manifest = manifest
	s.mu.Unlock()

	totalMs := float64(time.Since(start).Microseconds()) / 1000
	log.Printf("[snapshot] published %s: %d nodes, %d bytes compressed (query %.1fms, encode %.1fms, total %.1fms)",
		fname, len(nodes), len(compressed), queryMs, encodeMs, totalMs)
	s.pruneOld(fname)
	return nil
}

// collectNodes assembles the compact node tuples for the snapshot. Live nodes
// take priority over imported ones on duplicate public keys. Tuple layout:
// [pubkey, name, nodeType, lat, lon, lastAdvertAt, advertCount, networks[], freqMHz, flags[]].
// freqMHz is the imported node's radio frequency (0 for live nodes, which the
// client bands via their network membership instead). flags carries the
// Flagger's quality tags for live nodes; imported nodes are never flagged.
func (s *MapSnapshotter) collectNodes() [][10]any {
	imported := s.imported.Records()

	s.nodes.mu.Lock()
	out := make([][10]any, 0, len(s.nodes.nodes)+len(imported))
	seen := make(map[string]bool, len(s.nodes.nodes))
	for _, n := range s.nodes.nodes {
		if !n.HasGPS || !validCoords(n.Lat, n.Lon) {
			continue
		}
		// Mark seen before any skip so a matching imported directory entry never
		// slips back in under the same pubkey.
		seen[n.PubKey] = true
		// A node with an untrustworthy location (bogus coordinates, or an impossible
		// multi-network membership) is intentionally omitted: its coordinates would
		// misplace it on the map.
		if locationFlagged(n.Flags) {
			continue
		}
		nets := append([]string(nil), n.Networks...)
		if nets == nil {
			nets = []string{}
		}
		flags := append([]string(nil), n.Flags...)
		if flags == nil {
			flags = []string{}
		}
		out = append(out, [10]any{n.PubKey, n.Name, n.NodeType, n.Lat, n.Lon, n.LastAdvertAt, n.AdvertCount, nets, float64(0), flags})
	}
	s.nodes.mu.Unlock()

	for _, n := range imported {
		if seen[n.PublicKey] || !n.hasCoords() {
			continue
		}
		out = append(out, [10]any{n.PublicKey, n.AdvName, byte(n.Type), n.AdvLat, n.AdvLon, n.lastAdvertUnix(), uint64(0), []string{}, n.frequencyMHz(), []string{}})
	}
	return out
}

// pruneOld removes snapshot files older than the two most recent, preserving
// current (just written) and its predecessor.
func (s *MapSnapshotter) pruneOld(current string) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		log.Printf("[snapshot] reading dir for pruning: %v", err)
		return
	}
	var snaps []string
	for _, e := range entries {
		n := e.Name()
		if strings.HasPrefix(n, "map-") && strings.HasSuffix(n, ".json.zst") {
			snaps = append(snaps, n)
		}
	}
	sort.Strings(snaps) // ISO timestamps sort chronologically
	if len(snaps) <= 2 {
		return
	}
	for _, name := range snaps[:len(snaps)-2] {
		p := filepath.Join(s.dir, name)
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			log.Printf("[snapshot] deleting old snapshot %s: %v", name, err)
		}
	}
}

// CurrentManifest returns the most recently published manifest, or nil if no
// snapshot has been generated yet this process lifetime.
func (s *MapSnapshotter) CurrentManifest() *SnapshotManifest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.manifest
}

// ServeSnapshot serves a snapshot file at its logical URL (without .zst).
// Returns 406 when the client does not advertise zstd support, 404 when the
// requested snapshot no longer exists (pruned or never generated).
func (s *MapSnapshotter) ServeSnapshot(w http.ResponseWriter, r *http.Request) {
	name := filepath.Base(r.URL.Path) // "map-<ts>.json"
	if !strings.HasPrefix(name, "map-") || !strings.HasSuffix(name, ".json") {
		http.NotFound(w, r)
		return
	}
	if !strings.Contains(r.Header.Get("Accept-Encoding"), "zstd") {
		http.Error(w, "zstd encoding required", http.StatusNotAcceptable)
		return
	}
	zstPath := filepath.Join(s.dir, name+".zst")
	f, err := os.Open(zstPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.NotFound(w, r)
		} else {
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Content-Encoding", "zstd")
	h.Set("Cache-Control", "public, max-age=31536000, immutable")
	h.Set("Content-Length", strconv.FormatInt(fi.Size(), 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, f)
}

// ServeManifest serves latest.json from disk with a short cache lifetime.
func (s *MapSnapshotter) ServeManifest(w http.ResponseWriter, r *http.Request) {
	data, err := os.ReadFile(filepath.Join(s.dir, "latest.json"))
	if err != nil {
		if os.IsNotExist(err) {
			http.NotFound(w, r)
		} else {
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Cache-Control", "public, max-age=30")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}
