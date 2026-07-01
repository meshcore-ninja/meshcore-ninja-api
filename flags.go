package main

import (
	"context"
	"log"
	"sync"
	"time"
)

// Node/link flags are short machine-readable tags computed by the Flagger's
// periodic scan and surfaced to the map and other consumers (node API, snapshot,
// /api/flags) so they can style or hide suspect data. New rules can be added to
// the scan over time without changing how flags are transported.
const (
	// FlagFarFromNetwork marks a node whose GPS position is implausibly far from
	// the coverage area of every network it has been heard on — a likely-bogus
	// location (bad GPS fix, spoofed coordinates, or a misattributed advert).
	FlagFarFromNetwork = "far_from_network"
)

// defaultFarFromNetworkKM is the distance beyond which a node counts as too far
// from a network's coverage area to be plausible.
const defaultFarFromNetworkKM = 1000.0

// Flagger periodically scans nodes (and, in future, links), recomputes their
// flags, and writes the results back into the registry so every consumer sees a
// consistent set. It runs on its own ticker, independent of the ingest path.
type Flagger struct {
	nodes *NodeRegistry
	areas *NetworkAreas
	farKM float64

	mu       sync.Mutex
	lastScan time.Time // guarded: written by the scan loop, read by /api/flags
}

// ThresholdKM returns the far-from-network distance threshold in kilometers.
func (f *Flagger) ThresholdKM() float64 { return f.farKM }

// LastScan returns the time of the most recent completed scan (zero if none).
func (f *Flagger) LastScan() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastScan
}

// NewFlagger builds a flagger. farKM is the far-from-network threshold in
// kilometers; a non-positive value falls back to the default.
func NewFlagger(nodes *NodeRegistry, areas *NetworkAreas, farKM float64) *Flagger {
	if farKM <= 0 {
		farKM = defaultFarFromNetworkKM
	}
	return &Flagger{nodes: nodes, areas: areas, farKM: farKM}
}

// Run waits for startupDone (persisted state restored, areas loaded), runs an
// initial scan, then rescans on every interval tick until ctx is cancelled.
func (f *Flagger) Run(ctx context.Context, startupDone <-chan struct{}, interval time.Duration) {
	select {
	case <-ctx.Done():
		return
	case <-startupDone:
	}
	f.scanOnce()
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.scanOnce()
		}
	}
}

// scanOnce recomputes every node's flags and applies the changes to the registry.
func (f *Flagger) scanOnce() {
	start := time.Now()
	// No coverage areas loaded means the far-from-network rule can't be evaluated;
	// skip rather than clearing every node's flags on a transient load failure.
	if f.areas == nil || f.areas.Count() == 0 {
		return
	}

	desired := make(map[string][]string)
	flagged := 0
	f.nodes.eachNode(func(n *NodeRecord) {
		var flags []string
		if f.isFarFromNetwork(n) {
			flags = append(flags, FlagFarFromNetwork)
		}
		desired[n.PubKey] = flags
		if len(flags) > 0 {
			flagged++
		}
	})

	changed := f.nodes.ApplyFlags(desired)
	f.mu.Lock()
	f.lastScan = time.Now()
	f.mu.Unlock()
	log.Printf("[flags] scan complete: %d flagged, %d changed (%.1fms)",
		flagged, changed, float64(time.Since(start).Microseconds())/1000)
}

// isFarFromNetwork reports whether the node is beyond the threshold from the
// coverage area of every network it belongs to. A node is only flagged when at
// least one of its networks has a known area and it is far from all of them;
// nodes without GPS, without networks, or whose networks all lack coverage areas
// are left unflagged (their location can't be judged).
func (f *Flagger) isFarFromNetwork(n *NodeRecord) bool {
	if !n.HasGPS || !validCoords(n.Lat, n.Lon) || len(n.Networks) == 0 {
		return false
	}
	evaluated := false
	for _, netID := range n.Networks {
		d, ok := f.areas.DistanceKM(netID, n.Lat, n.Lon)
		if !ok {
			continue
		}
		evaluated = true
		if d <= f.farKM {
			return false // close enough to at least one of its networks
		}
	}
	return evaluated // far from all networks that had an area to check against
}
