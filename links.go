package main

import (
	"bytes"
	"encoding/hex"
	"math"
	"sort"
	"strings"
	"sync"
)

// Observed node links (internally "edges"). A link is an undirected pair of mesh
// nodes that appeared *adjacent* in a packet's resolved path. The registry is
// global — shared by every collector across every network — mirroring the global
// node and observer registries, not living inside any NetworkState.
//
// Only adjacent entries in resolved_path create a link: a path A→B→C→D yields
// A—B, B—C, C—D and nothing else (no A—C, A—D, B—D). Links are undirected, so
// A—B == B—A. When the packet observer is itself a full node pubkey, the
// collector appends it as the final receiving hop before recording links.

// defaultLinkHalfLife is the half-life (seconds) of a link's recent-activity
// score: 24h by default. Each globally-deduplicated packet-link event adds 1 to
// the score, which then decays exponentially so recent traffic dominates.
const defaultLinkHalfLife = 24 * 60 * 60

// linkShards is the number of independently-locked shards. Many collectors update
// the registry concurrently, so the keyspace is split to keep lock contention low.
const linkShards = 64

const linkSNRHistoryLimit = 5

// pubKey is a node's 32-byte Ed25519 public key in raw (decoded) form.
type pubKey [32]byte

// linkKey is the canonical, fixed-size key for an undirected link: the two node
// public keys in byte order (smaller first). Being a comparable array it is used
// directly as a map key, with no per-event string concatenation or allocation.
type linkKey [64]byte

// normalizePub validates and decodes a public key from hex. Keys are 32-byte
// Ed25519 keys (64 hex chars); anything else is rejected.
func normalizePub(s string) (pubKey, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	if len(s) != 64 {
		return pubKey{}, false
	}
	var k pubKey
	if _, err := hex.Decode(k[:], []byte(s)); err != nil {
		return pubKey{}, false
	}
	return k, true
}

// canonicalKey orders two node keys into the undirected canonical link key. ok is
// false for a self-link (a == b), which is never recorded.
func canonicalKey(a, b pubKey) (linkKey, bool) {
	if a == b {
		return linkKey{}, false
	}
	var k linkKey
	if bytes.Compare(a[:], b[:]) <= 0 {
		copy(k[:32], a[:])
		copy(k[32:], b[:])
	} else {
		copy(k[:32], b[:])
		copy(k[32:], a[:])
	}
	return k, true
}

func (k linkKey) nodeA() string { return hex.EncodeToString(k[:32]) }
func (k linkKey) nodeB() string { return hex.EncodeToString(k[32:]) }

// linkNetwork records that a link was observed through a given network, with the
// first/last times it was seen there. The set of these is what lets a link report
// every network it has carried traffic on, independent of the global packet count.
type linkNetwork struct {
	NetworkID string
	FirstSeen int64
	LastSeen  int64
}

// linkPosEpsilonKM is how far either endpoint must move before the link's
// current position segment is closed and a new one opened. Below this, movement
// is treated as GPS jitter and folded into the current segment.
const linkPosEpsilonKM = 1.0

// linkPosSegment is one period during which both endpoints stayed put (within
// linkPosEpsilonKM). When a node moves, the live segment is frozen into history
// so the link's earlier geometry is kept "for the record".
type linkPosSegment struct {
	LatA, LonA  float64
	LatB, LonB  float64
	FirstSeen   int64
	LastSeen    int64
	PacketCount uint64
}

// LinkRecord is the sparse aggregate for one observed link. Only links that have
// actually been seen exist — there is no N×N matrix. Storage grows solely with
// observed links.
type LinkRecord struct {
	Key            linkKey
	NodeA          string // lowercase hex, canonical order (== Key.nodeA)
	NodeB          string // lowercase hex, canonical order (== Key.nodeB)
	PacketCount    uint64 // global count after cross-network/observer dedup
	FirstSeen      int64
	LastSeen       int64
	Score          float64 // recent-activity score at ScoreUpdatedAt (pre-decay)
	ScoreUpdatedAt int64
	Networks       []linkNetwork // networks this link was observed through

	// Directional counts: DirAB is events seen travelling canonical NodeA→NodeB
	// (i.e. B received the packet from A), DirBA the reverse. They sum to
	// PacketCount and expose asymmetric links (who relays to whom).
	DirAB uint64
	DirBA uint64
	// LastHashAB / LastHashBA are the most recent packet hashes counted in each
	// canonical direction, so a suspicious directional link can be traced back to
	// a concrete source packet.
	LastHashAB string
	LastHashBA string
	// Per-direction SNR history (dB), best-effort from a TRACE packet's per-hop
	// accumulator. SNR is measured at the receiving end, so it is inherently
	// directional: SNRsAB is what NodeB heard when receiving from NodeA (A->B),
	// SNRsBA what NodeA heard from NodeB. Each slice keeps the last few values in
	// observation order.
	SNRsAB []float64
	SNRsBA []float64
	// SourceCounts breaks the counted events down by route type
	// ("flood", "direct", "transport_flood", "transport_direct", "unknown"), for
	// diagnosing which packet kinds a link's adjacency actually came from —
	// flood/trace paths are observed adjacency, direct paths are declared routes.
	SourceCounts map[string]uint64

	// Current position segment: the endpoints' positions while stationary. Valid
	// only when HasPos (both endpoints had known GPS at observation time).
	HasPos         bool
	LatA, LonA     float64
	LatB, LonB     float64
	SegFirstSeen   int64
	SegPacketCount uint64
	// Segments holds prior (frozen) position segments, oldest first — the link's
	// geometry history across endpoint moves.
	Segments []linkPosSegment

	dirty bool // pending persistence
}

// linkDedupKey is the global packet-link deduplication key: (packet content
// hash, canonical link). It deliberately excludes network and observer ids so the
// same packet reported by multiple observers, or across multiple networks, counts
// the link's activity only once.
type linkDedupKey struct {
	hash string
	key  linkKey
}

type linkShard struct {
	mu    sync.Mutex
	links map[linkKey]*LinkRecord
	dedup map[linkDedupKey]int64 // -> last-seen unix; short-lived, swept by window
}

// LinkRegistry is the global, concurrency-safe store of observed links. It is
// sharded by link key so concurrent collectors rarely contend on the same lock.
type LinkRegistry struct {
	shards   [linkShards]linkShard
	halfLife float64 // seconds

	// Routing reuses a briefly-cached snapshot of the whole link graph so a burst
	// of hovers doesn't rebuild adjacency from every shard each time. See route.go.
	routeMu  sync.Mutex
	routeAdj *routeAdjacency
}

func newLinkRegistry(halfLifeSeconds float64) *LinkRegistry {
	if halfLifeSeconds <= 0 {
		halfLifeSeconds = defaultLinkHalfLife
	}
	r := &LinkRegistry{halfLife: halfLifeSeconds}
	for i := range r.shards {
		r.shards[i].links = make(map[linkKey]*LinkRecord)
		r.shards[i].dedup = make(map[linkDedupKey]int64)
	}
	return r
}

// shardFor picks a shard from the link key. Public keys are uniformly random, so
// a couple of key bytes spread links evenly across shards.
func (r *LinkRegistry) shardFor(k linkKey) *linkShard {
	return &r.shards[(uint(k[0])<<8|uint(k[1]))%linkShards]
}

// ObservePath records the adjacent links of one packet's resolved path. It is the
// single entry point from the collector. The packet hash drives global
// deduplication; networkID is recorded on every touched link; now is the receive
// time.
//
// Processing per the spec: normalize and validate keys, collapse consecutive
// duplicate nodes, ignore self-links, and dedup repeated links within the path.
// Network association is updated even when the (hash, link) pair was already
// counted, so a link records every network it was heard on without inflating its
// global packet count.
// PathObservation carries everything ObservePathCtx needs to record the links of
// one packet's resolved path, including the diagnostic context (route type,
// per-hop SNRs, endpoint positions) beyond the bare adjacency.
type PathObservation struct {
	Hash      string
	NetworkID string
	RouteType string // "flood","direct","transport_flood","transport_direct"; "" = unknown
	Path      []string
	// SNRs, when non-nil, are SNR values (dB) decoded from a TRACE packet's
	// accumulator. Some TRACE records include a leading sample for the origin node
	// even though resolved_path starts at the first relay, so attribution
	// right-aligns samples to the accepted links.
	SNRs []float64
	Now  int64
	// PosOf resolves a node's current position; nil disables position stamping.
	PosOf func(pubkeyHex string) (lat, lon float64, ok bool)
}

// ObservePath records the adjacent links of one packet's resolved path with no
// extra context. Retained for callers (and tests) that only have the bare path.
func (r *LinkRegistry) ObservePath(hash, networkID string, path []string, now int64) {
	r.ObservePathCtx(PathObservation{Hash: hash, NetworkID: networkID, Path: path, Now: now})
}

// ObservePathCtx records the adjacent links of one packet's resolved path. It is
// the single entry point from the collector. The packet hash drives global
// deduplication; networkID is recorded on every touched link; Now is the receive
// time. Direction (from→to) is taken from path order, positions from PosOf, and
// SNRs (if present) attributed to the receiving hop.
//
// Processing per the spec: normalize and validate keys, collapse consecutive
// duplicate nodes, ignore self-links, and dedup repeated links within the path.
func (r *LinkRegistry) ObservePathCtx(o PathObservation) {
	hash := strings.ToLower(strings.TrimSpace(o.Hash))
	if hash == "" || len(o.Path) < 2 {
		return
	}

	// Walk adjacent pairs of the (consecutive-dedup'd) path. We keep a per-path
	// set so a link repeated within one path is handled once. prev/cur order is
	// the propagation direction: cur received the packet from prev.
	var prev pubKey
	var prevHex string
	var havePrev bool
	var prevRaw string
	hop := 0 // count of accepted (normalized, de-duplicated) nodes so far
	seen := make(map[linkKey]struct{})
	snrBase := snrBaseIndex(o.Path, o.SNRs)

	for _, raw := range o.Path {
		raw = strings.ToLower(strings.TrimSpace(raw))
		if raw == "" {
			continue
		}
		// Collapse consecutive duplicate nodes (A,A,B -> A,B).
		if havePrev && raw == prevRaw {
			continue
		}
		cur, ok := normalizePub(raw)
		if !ok {
			// Invalid key breaks adjacency: the links touching it are dropped and
			// no link is inferred across it.
			havePrev = false
			prevRaw = raw
			continue
		}
		if havePrev {
			if key, ok := canonicalKey(prev, cur); ok {
				if _, dup := seen[key]; !dup {
					seen[key] = struct{}{}
					obs := linkObs{
						key:       key,
						fromIsA:   bytes.Equal(key[:32], prev[:]), // prev is canonical NodeA?
						hash:      hash,
						networkID: o.NetworkID,
						routeType: o.RouteType,
						now:       o.Now,
					}
					obs.setPositions(o.PosOf, prevHex, raw)
					// Right-align SNRs to accepted links so a TRACE origin sample
					// does not shift every relay link by one.
					if idx := snrBase + hop - 1; idx >= 0 && idx < len(o.SNRs) {
						obs.snr, obs.hasSNR = o.SNRs[idx], true
					}
					r.observeLink(obs)
				}
			}
		}
		prev = cur
		prevHex = raw
		prevRaw = raw
		havePrev = true
		hop++
	}
}

func snrBaseIndex(path []string, snrs []float64) int {
	if len(snrs) == 0 {
		return 0
	}
	links := acceptedLinkCount(path)
	if links <= 0 || len(snrs) <= links {
		return 0
	}
	return len(snrs) - links
}

func acceptedLinkCount(path []string) int {
	var prev pubKey
	var havePrev bool
	var prevRaw string
	links := 0
	for _, raw := range path {
		raw = strings.ToLower(strings.TrimSpace(raw))
		if raw == "" {
			continue
		}
		if havePrev && raw == prevRaw {
			continue
		}
		cur, ok := normalizePub(raw)
		if !ok {
			havePrev = false
			prevRaw = raw
			continue
		}
		if havePrev {
			if _, ok := canonicalKey(prev, cur); ok {
				links++
			}
		}
		prev = cur
		prevRaw = raw
		havePrev = true
	}
	return links
}

// linkObs is one resolved adjacency to record: the canonical link key, the
// propagation direction, and the diagnostic context for this observation.
type linkObs struct {
	key        linkKey
	fromIsA    bool // whether the "from" (sending) endpoint is canonical NodeA
	hash       string
	networkID  string
	routeType  string
	now        int64
	hasPos     bool
	latA, lonA float64
	latB, lonB float64
	snr        float64
	hasSNR     bool
}

// setPositions resolves both endpoints' positions (in canonical A/B order) via
// posOf. Positions are only stamped when both endpoints have a known location.
func (o *linkObs) setPositions(posOf func(string) (float64, float64, bool), fromHex, toHex string) {
	if posOf == nil {
		return
	}
	fLat, fLon, fok := posOf(fromHex)
	tLat, tLon, tok := posOf(toHex)
	if !fok || !tok {
		return
	}
	o.hasPos = true
	if o.fromIsA {
		o.latA, o.lonA, o.latB, o.lonB = fLat, fLon, tLat, tLon
	} else {
		o.latA, o.lonA, o.latB, o.lonB = tLat, tLon, fLat, fLon
	}
}

// observeLink applies one (already de-duplicated within its path) adjacent link
// observation: upserts the record, always records the network, and increments the
// global packet count + activity score only when this (hash, link) pair is new
// within the dedup window. Direction, source route type, last hash, SNR, and
// position segments are updated on the same counted events.
func (r *LinkRegistry) observeLink(o linkObs) {
	sh := r.shardFor(o.key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	rec := sh.links[o.key]
	if rec == nil {
		rec = &LinkRecord{
			Key:            o.key,
			NodeA:          o.key.nodeA(),
			NodeB:          o.key.nodeB(),
			FirstSeen:      o.now,
			ScoreUpdatedAt: o.now,
		}
		sh.links[o.key] = rec
	}

	// Network association is updated regardless of dedup so the link records every
	// network it has carried this packet (and others) on.
	if o.networkID != "" {
		rec.addNetwork(o.networkID, o.now)
		rec.dirty = true
	}

	dk := linkDedupKey{hash: o.hash, key: o.key}
	if _, dup := sh.dedup[dk]; dup {
		sh.dedup[dk] = o.now // refresh so an actively-reflooded packet stays deduped
		return
	}
	sh.dedup[dk] = o.now

	// New globally-deduplicated packet-link event. Stamp position (against the
	// still-current LastSeen) before advancing counters so a closed segment keeps
	// its true last-seen time.
	if o.hasPos {
		rec.updatePosition(o.latA, o.lonA, o.latB, o.lonB, o.now)
	}
	rec.PacketCount++
	rec.LastSeen = o.now
	rec.decayScore(o.now, r.halfLife)
	rec.Score++
	if o.fromIsA {
		rec.DirAB++
		rec.LastHashAB = o.hash
		if o.hasSNR {
			rec.SNRsAB = appendCappedFloat(rec.SNRsAB, o.snr, linkSNRHistoryLimit)
		}
	} else {
		rec.DirBA++
		rec.LastHashBA = o.hash
		if o.hasSNR {
			rec.SNRsBA = appendCappedFloat(rec.SNRsBA, o.snr, linkSNRHistoryLimit)
		}
	}
	rt := o.routeType
	if rt == "" {
		rt = "unknown"
	}
	if rec.SourceCounts == nil {
		rec.SourceCounts = make(map[string]uint64, 2)
	}
	rec.SourceCounts[rt]++
	rec.dirty = true
}

// updatePosition folds a new endpoint-position observation into the link's
// current segment, or — when either endpoint has moved beyond linkPosEpsilonKM —
// freezes the current segment into history and opens a fresh one. Called before
// the counters advance, so a frozen segment's LastSeen reflects its last
// stationary observation.
func (rec *LinkRecord) updatePosition(latA, lonA, latB, lonB float64, now int64) {
	if !rec.HasPos {
		rec.HasPos = true
		rec.LatA, rec.LonA, rec.LatB, rec.LonB = latA, lonA, latB, lonB
		rec.SegFirstSeen = now
		rec.SegPacketCount = 1
		return
	}
	moved := haversineKM(rec.LatA, rec.LonA, latA, lonA) > linkPosEpsilonKM ||
		haversineKM(rec.LatB, rec.LonB, latB, lonB) > linkPosEpsilonKM
	if moved {
		rec.Segments = append(rec.Segments, linkPosSegment{
			LatA: rec.LatA, LonA: rec.LonA, LatB: rec.LatB, LonB: rec.LonB,
			FirstSeen: rec.SegFirstSeen, LastSeen: rec.LastSeen, PacketCount: rec.SegPacketCount,
		})
		rec.LatA, rec.LonA, rec.LatB, rec.LonB = latA, lonA, latB, lonB
		rec.SegFirstSeen = now
		rec.SegPacketCount = 1
		return
	}
	rec.SegPacketCount++
}

// addNetwork records (or refreshes) the network association on a link, keeping
// first-seen order like the node/observer registries.
func (rec *LinkRecord) addNetwork(networkID string, now int64) {
	for i := range rec.Networks {
		if rec.Networks[i].NetworkID == networkID {
			rec.Networks[i].LastSeen = now
			return
		}
	}
	rec.Networks = append(rec.Networks, linkNetwork{NetworkID: networkID, FirstSeen: now, LastSeen: now})
}

// decayScore decays the stored score to `now` using the configured half-life,
// then advances ScoreUpdatedAt. Caller adds the new event's weight afterwards.
func (rec *LinkRecord) decayScore(now int64, halfLife float64) {
	if rec.ScoreUpdatedAt == 0 || halfLife <= 0 {
		rec.ScoreUpdatedAt = now
		return
	}
	if dt := now - rec.ScoreUpdatedAt; dt > 0 {
		rec.Score *= math.Exp2(-float64(dt) / halfLife)
	}
	rec.ScoreUpdatedAt = now
}

// decayedScore returns the score decayed to `now` without mutating the record.
func decayedScore(score float64, updatedAt, now int64, halfLife float64) float64 {
	if halfLife <= 0 || updatedAt == 0 {
		return score
	}
	if dt := now - updatedAt; dt > 0 {
		return score * math.Exp2(-float64(dt)/halfLife)
	}
	return score
}

// sweep drops packet-link dedup entries older than the window so the short-lived
// cache stays bounded. The aggregates themselves are never aged out.
func (r *LinkRegistry) sweep(now, dedupWindow int64) {
	for i := range r.shards {
		sh := &r.shards[i]
		sh.mu.Lock()
		for k, ts := range sh.dedup {
			if now-ts > dedupWindow {
				delete(sh.dedup, k)
			}
		}
		sh.mu.Unlock()
	}
}

// --- API view shapes ---

// LinkNeighbor is one link from a selected node's perspective: the aggregate
// stats plus the *other* endpoint's public key. Neighbor metadata (name, type,
// gps) is resolved by the HTTP handler via the node registry.
type LinkNeighbor struct {
	Neighbor       string   // hex pubkey of the other endpoint
	PacketCount    uint64   // global, deduplicated
	RecentActivity float64  // decayed score at query time
	FirstSeen      int64    // global first observation of the link
	LastSeen       int64    // global last counted observation
	Networks       []string // networks the link was observed through, first-seen order

	// Direction relative to the selected node: SentByNode counts events where the
	// node was the sender (node→neighbor), RecvByNode where it received
	// (neighbor→node). They sum to PacketCount.
	SentByNode         uint64
	RecvByNode         uint64
	LastHashSentByNode string
	LastHashRecvByNode string
	LastSNRSentByNode  float64
	HasSNRSentByNode   bool
	LastSNRRecvByNode  float64
	HasSNRRecvByNode   bool
	SNRSentByNode      []float64         // last SNRs for node->neighbor direction, oldest first
	SNRRecvByNode      []float64         // last SNRs for neighbor->node direction, oldest first
	Sources            map[string]uint64 // counted events by route type

	// Geometry from the current position segment, in the selected node's frame.
	HasPos       bool
	NodeLat      float64
	NodeLon      float64
	NeighborLat  float64
	NeighborLon  float64
	Moved        bool // true when an endpoint has moved (prior segments exist)
	SegmentCount int  // number of frozen historical segments
}

// LinksForNode returns every observed link with the given node as an endpoint,
// each described from that node's side (the neighbor is the opposite endpoint).
// The activity score is decayed to `now`. Storage is scanned sparsely — only
// links that actually exist are visited — and no global topology is materialized.
func (r *LinkRegistry) LinksForNode(node pubKey, now int64) []LinkNeighbor {
	var out []LinkNeighbor
	for i := range r.shards {
		sh := &r.shards[i]
		sh.mu.Lock()
		for key, rec := range sh.links {
			var neighbor pubKey
			// nodeIsA: the selected node is canonical NodeA, so its outbound
			// direction (node→neighbor) is DirAB and its position is (LatA,LonA).
			var nodeIsA bool
			switch {
			case bytes.Equal(key[:32], node[:]):
				copy(neighbor[:], key[32:])
				nodeIsA = true
			case bytes.Equal(key[32:], node[:]):
				copy(neighbor[:], key[:32])
			default:
				continue
			}
			nets := make([]string, len(rec.Networks))
			for j, n := range rec.Networks {
				nets[j] = n.NetworkID
			}
			ln := LinkNeighbor{
				Neighbor:       hex.EncodeToString(neighbor[:]),
				PacketCount:    rec.PacketCount,
				RecentActivity: decayedScore(rec.Score, rec.ScoreUpdatedAt, now, r.halfLife),
				FirstSeen:      rec.FirstSeen,
				LastSeen:       rec.LastSeen,
				Networks:       nets,
				Sources:        copyCounts(rec.SourceCounts),
				HasPos:         rec.HasPos,
				Moved:          len(rec.Segments) > 0,
				SegmentCount:   len(rec.Segments),
			}
			if nodeIsA {
				ln.SentByNode, ln.RecvByNode = rec.DirAB, rec.DirBA
				ln.LastHashSentByNode, ln.LastHashRecvByNode = rec.LastHashAB, rec.LastHashBA
				ln.SNRSentByNode = append([]float64(nil), rec.SNRsAB...)
				ln.SNRRecvByNode = append([]float64(nil), rec.SNRsBA...)
				ln.NodeLat, ln.NodeLon, ln.NeighborLat, ln.NeighborLon = rec.LatA, rec.LonA, rec.LatB, rec.LonB
			} else {
				ln.SentByNode, ln.RecvByNode = rec.DirBA, rec.DirAB
				ln.LastHashSentByNode, ln.LastHashRecvByNode = rec.LastHashBA, rec.LastHashAB
				ln.SNRSentByNode = append([]float64(nil), rec.SNRsBA...)
				ln.SNRRecvByNode = append([]float64(nil), rec.SNRsAB...)
				ln.NodeLat, ln.NodeLon, ln.NeighborLat, ln.NeighborLon = rec.LatB, rec.LonB, rec.LatA, rec.LonA
			}
			if len(ln.SNRSentByNode) > 0 {
				ln.LastSNRSentByNode = ln.SNRSentByNode[len(ln.SNRSentByNode)-1]
				ln.HasSNRSentByNode = true
			}
			if len(ln.SNRRecvByNode) > 0 {
				ln.LastSNRRecvByNode = ln.SNRRecvByNode[len(ln.SNRRecvByNode)-1]
				ln.HasSNRRecvByNode = true
			}
			out = append(out, ln)
		}
		sh.mu.Unlock()
	}
	return out
}

// --- persistence plumbing ---

// TakeDirty returns deep copies of every link changed since the last flush and
// clears their dirty flags atomically. On a failed persist the caller re-marks
// them with Requeue so the change isn't lost. Concurrent updates set the flag
// again and are picked up on the next cycle.
func (r *LinkRegistry) TakeDirty() []LinkRecord {
	var out []LinkRecord
	for i := range r.shards {
		sh := &r.shards[i]
		sh.mu.Lock()
		for _, rec := range sh.links {
			if !rec.dirty {
				continue
			}
			cp := *rec
			cp.Networks = append([]linkNetwork(nil), rec.Networks...)
			cp.Segments = append([]linkPosSegment(nil), rec.Segments...)
			cp.SNRsAB = append([]float64(nil), rec.SNRsAB...)
			cp.SNRsBA = append([]float64(nil), rec.SNRsBA...)
			cp.SourceCounts = copyCounts(rec.SourceCounts)
			out = append(out, cp)
			rec.dirty = false
		}
		sh.mu.Unlock()
	}
	return out
}

// Requeue re-marks the given links dirty after a failed persist so they flush on
// the next cycle.
func (r *LinkRegistry) Requeue(records []LinkRecord) {
	for i := range records {
		sh := r.shardFor(records[i].Key)
		sh.mu.Lock()
		if rec := sh.links[records[i].Key]; rec != nil {
			rec.dirty = true
		}
		sh.mu.Unlock()
	}
}

// Restore seeds the registry from persisted aggregates at startup, before any
// collector runs. The short-lived packet-link dedup cache starts empty (like the
// counter dedup state) and rebuilds within the window.
func (r *LinkRegistry) Restore(records []LinkRecord) {
	for i := range records {
		rec := records[i]
		key, ok := keyFromPair(rec.NodeA, rec.NodeB)
		if !ok {
			continue
		}
		rec.Key = key
		rec.NodeA = key.nodeA()
		rec.NodeB = key.nodeB()
		rec.dirty = false
		sh := r.shardFor(key)
		sh.mu.Lock()
		if existing := sh.links[key]; existing != nil {
			mergeRestoredLink(existing, &rec, r.halfLife)
		} else {
			sh.links[key] = &rec
		}
		sh.mu.Unlock()
	}
	r.routeMu.Lock()
	r.routeAdj = nil
	r.routeMu.Unlock()
}

func mergeRestoredLink(existing, restored *LinkRecord, halfLife float64) {
	existingLast := existing.LastSeen // capture before it advances below

	existing.PacketCount += restored.PacketCount
	existing.DirAB += restored.DirAB
	existing.DirBA += restored.DirBA
	if existing.FirstSeen == 0 || (restored.FirstSeen > 0 && restored.FirstSeen < existing.FirstSeen) {
		existing.FirstSeen = restored.FirstSeen
	}
	if restored.LastSeen > existing.LastSeen {
		existing.LastSeen = restored.LastSeen
	}
	existing.Score = combinedScore(existing.Score, existing.ScoreUpdatedAt, restored.Score, restored.ScoreUpdatedAt, halfLife)
	if restored.ScoreUpdatedAt > existing.ScoreUpdatedAt {
		existing.ScoreUpdatedAt = restored.ScoreUpdatedAt
	}
	for _, n := range restored.Networks {
		existing.mergeNetwork(n)
	}
	if restored.LastSeen >= existingLast {
		if restored.LastHashAB != "" {
			existing.LastHashAB = restored.LastHashAB
		}
		if restored.LastHashBA != "" {
			existing.LastHashBA = restored.LastHashBA
		}
	}
	restoredNewer := restored.LastSeen >= existingLast
	existing.SNRsAB = mergeSNRHistory(existing.SNRsAB, restored.SNRsAB, restoredNewer)
	existing.SNRsBA = mergeSNRHistory(existing.SNRsBA, restored.SNRsBA, restoredNewer)
	if existing.SourceCounts == nil && len(restored.SourceCounts) > 0 {
		existing.SourceCounts = make(map[string]uint64, len(restored.SourceCounts))
	}
	for k, v := range restored.SourceCounts {
		existing.SourceCounts[k] += v
	}
	// Fold restored geometry into history. The live side's current segment (if any)
	// wins as "current"; the restored current + its history become older segments.
	existing.Segments = append(existing.Segments, restored.Segments...)
	if restored.HasPos {
		existing.Segments = append(existing.Segments, linkPosSegment{
			LatA: restored.LatA, LonA: restored.LonA, LatB: restored.LatB, LonB: restored.LonB,
			FirstSeen: restored.SegFirstSeen, LastSeen: restored.LastSeen, PacketCount: restored.SegPacketCount,
		})
	}
	if !existing.HasPos && restored.HasPos {
		existing.HasPos = true
		existing.LatA, existing.LonA, existing.LatB, existing.LonB = restored.LatA, restored.LonA, restored.LatB, restored.LonB
		existing.SegFirstSeen, existing.SegPacketCount = restored.SegFirstSeen, restored.SegPacketCount
		// Drop the duplicate we just appended as history.
		existing.Segments = existing.Segments[:len(existing.Segments)-1]
	}
}

func appendCappedFloat(vals []float64, v float64, limit int) []float64 {
	vals = append(vals, v)
	if len(vals) <= limit {
		return vals
	}
	return append([]float64(nil), vals[len(vals)-limit:]...)
}

func appendCappedFloats(vals []float64, add []float64, limit int) []float64 {
	for _, v := range add {
		vals = appendCappedFloat(vals, v, limit)
	}
	return vals
}

func mergeSNRHistory(existing, restored []float64, restoredNewer bool) []float64 {
	if restoredNewer {
		return appendCappedFloats(existing, restored, linkSNRHistoryLimit)
	}
	vals := append([]float64(nil), restored...)
	vals = append(vals, existing...)
	if len(vals) <= linkSNRHistoryLimit {
		return vals
	}
	return append([]float64(nil), vals[len(vals)-linkSNRHistoryLimit:]...)
}

// copyCounts returns a shallow copy of a source-count map (nil stays nil), so
// persisted/served copies don't alias the live registry map.
func copyCounts(m map[string]uint64) map[string]uint64 {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func combinedScore(a float64, aAt int64, b float64, bAt int64, halfLife float64) float64 {
	if bAt > aAt {
		return decayedScore(a, aAt, bAt, halfLife) + b
	}
	return a + decayedScore(b, bAt, aAt, halfLife)
}

func (rec *LinkRecord) mergeNetwork(n linkNetwork) {
	for i := range rec.Networks {
		if rec.Networks[i].NetworkID == n.NetworkID {
			if rec.Networks[i].FirstSeen == 0 || (n.FirstSeen > 0 && n.FirstSeen < rec.Networks[i].FirstSeen) {
				rec.Networks[i].FirstSeen = n.FirstSeen
			}
			if n.LastSeen > rec.Networks[i].LastSeen {
				rec.Networks[i].LastSeen = n.LastSeen
			}
			return
		}
	}
	rec.Networks = append(rec.Networks, n)
}

// keyFromPair builds a canonical link key from two hex public keys.
func keyFromPair(a, b string) (linkKey, bool) {
	ka, ok := normalizePub(a)
	if !ok {
		return linkKey{}, false
	}
	kb, ok := normalizePub(b)
	if !ok {
		return linkKey{}, false
	}
	return canonicalKey(ka, kb)
}

// linkCount reports the number of stored links (testing/diagnostics).
func (r *LinkRegistry) linkCount() int {
	n := 0
	for i := range r.shards {
		sh := &r.shards[i]
		sh.mu.Lock()
		n += len(sh.links)
		sh.mu.Unlock()
	}
	return n
}

// sortNeighborsByActivity orders links by recent activity (desc), breaking ties
// by neighbor key for determinism.
func sortNeighborsByActivity(links []LinkNeighbor) {
	sort.Slice(links, func(i, j int) bool {
		if links[i].RecentActivity != links[j].RecentActivity {
			return links[i].RecentActivity > links[j].RecentActivity
		}
		return links[i].Neighbor < links[j].Neighbor
	})
}
