package main

import (
	"compress/gzip"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Server exposes the read-only REST API the frontend polls.
type Server struct {
	store       *Store
	nodes       *NodeRegistry
	observers   *ObserverRegistry
	links       *LinkRegistry
	imported    *ImportRegistry
	db          *DB // optional; enables the per-node advert history endpoint
	metrics     *Metrics
	hub         *Hub
	snapshotter *MapSnapshotter
	flagger     *Flagger // optional; backs /api/flags metadata
	allowOrigin string

	statsMu       sync.Mutex
	statsCached   statsResponse
	statsCachedAt time.Time
}

func NewServer(store *Store, nodes *NodeRegistry, observers *ObserverRegistry, links *LinkRegistry, imported *ImportRegistry, db *DB, metrics *Metrics, hub *Hub, snapshotter *MapSnapshotter, flagger *Flagger, allowOrigin string) *Server {
	return &Server{store: store, nodes: nodes, observers: observers, links: links, imported: imported, db: db, metrics: metrics, hub: hub, snapshotter: snapshotter, flagger: flagger, allowOrigin: allowOrigin}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	// Each route is instrumented under a fixed, normalized label so path
	// variables (network id, pubkey) never inflate metric cardinality.
	mux.HandleFunc("/api/health", s.instrument("/api/health", s.handleHealth))
	mux.HandleFunc("/api/stats", s.instrument("/api/stats", s.handleStats))
	mux.HandleFunc("/api/networks", s.instrument("/api/networks", s.handleNetworks))
	mux.HandleFunc("/api/networks/", s.instrument("/api/networks/:id", s.handleNetworkDetail))
	mux.HandleFunc("/api/nodes", s.instrument("/api/nodes", s.handleNodes))
	mux.HandleFunc("/api/nodes/", s.instrument("/api/nodes/:pubkey", s.handleNodeSub))
	mux.HandleFunc("/api/search/options", s.instrument("/api/search/options", s.handleSearchOptions))
	mux.HandleFunc("/api/search", s.instrument("/api/search", s.handleSearch))
	mux.HandleFunc("/api/route", s.instrument("/api/route", s.handleRoute))
	mux.HandleFunc("/api/observers", s.instrument("/api/observers", s.handleObservers))
	mux.HandleFunc("/api/flags", s.instrument("/api/flags", s.handleFlags))
	// Prometheus/VictoriaMetrics scrape endpoint. Left un-instrumented to avoid
	// the scraper polluting the API latency histograms.
	if s.metrics != nil {
		mux.HandleFunc("/metrics", s.handleMetrics)
	}
	wrapped := s.withCORS(gzipMiddleware(mux))

	// Snapshot and WebSocket routes live outside the gzip middleware. Snapshots
	// are already zstd-compressed on disk and must not be re-encoded; WebSocket
	// upgrades are incompatible with the gzip response writer.
	root := http.NewServeMux()
	root.Handle("/", wrapped)
	if s.snapshotter != nil {
		root.HandleFunc("/api/snapshots/latest.json", s.withCORSSingle(s.snapshotter.ServeManifest))
		root.HandleFunc("/api/snapshots/", s.withCORSSingle(s.snapshotter.ServeSnapshot))
	}
	if s.hub != nil {
		root.HandleFunc("/api/live", s.hub.ServeWS)
	}
	return root
}

// withCORSSingle wraps a single handler with CORS headers (no gzip).
func (s *Server) withCORSSingle(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", s.allowOrigin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Vary", "Origin")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h(w, r)
	}
}

// gzipMiddleware compresses responses for clients that accept gzip. The map
// "all nodes" payload is a few MB of JSON, so this is a meaningful win; small
// responses compress harmlessly.
func gzipMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Add("Vary", "Accept-Encoding")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		next.ServeHTTP(&gzipResponseWriter{ResponseWriter: w, gz: gz}, r)
	})
}

type gzipResponseWriter struct {
	http.ResponseWriter
	gz *gzip.Writer
}

func (g *gzipResponseWriter) Write(b []byte) (int, error) {
	// Content-Length would describe the uncompressed size; drop it.
	g.Header().Del("Content-Length")
	return g.gz.Write(b)
}

func (s *Server) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", s.allowOrigin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Vary", "Origin")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// --- response shapes ---

type networkSummary struct {
	ID                 string  `json:"id"`
	Name               string  `json:"name"`
	PktPerMin          float64 `json:"pktPerMin"`
	UniquePackets      uint64  `json:"uniquePackets"`
	Observations       uint64  `json:"observations"`
	Observers          int     `json:"observers"`
	Nodes              int     `json:"nodes"`
	AnalyzersTotal     int     `json:"analyzersTotal"`
	AnalyzersConnected int     `json:"analyzersConnected"`
	LastPacketAt       int64   `json:"lastPacketAt"`
}

type statsResponse struct {
	Nodes     statsNodeCounts `json:"nodes"`
	Directory directoryStats  `json:"directory"`
	SQLite    *SQLiteStats    `json:"sqlite,omitempty"`
}

type statsNodeCounts struct {
	Live     int `json:"live"`
	Imported int `json:"imported"`
	Total    int `json:"total"`
}

type directoryStats struct {
	Total     int                  `json:"total"`
	Sources   directorySourceStats `json:"sources"`
	Types     directoryTypeStats   `json:"types"`
	Freshness directoryFreshStats  `json:"freshness"`
	Data      directoryDataStats   `json:"data"`
}

type directorySourceStats struct {
	Advert    int `json:"advert"`
	Map       int `json:"map"`
	CoreScope int `json:"corescope"`
}

type directoryTypeStats struct {
	Unknown   int `json:"unknown"`
	Companion int `json:"companion"`
	Repeater  int `json:"repeater"`
	Room      int `json:"room"`
	Sensor    int `json:"sensor"`
}

type directoryFreshStats struct {
	Last24h      int `json:"last24h"`
	Last7d       int `json:"last7d"`
	OlderThan30d int `json:"olderThan30d"`
}

type directoryDataStats struct {
	WithLocation int `json:"withLocation"`
	WithName     int `json:"withName"`
}

const statsCacheTTL = 15 * time.Second

func (s *Server) statsSnapshot() statsResponse {
	now := time.Now()
	s.statsMu.Lock()
	if !s.statsCachedAt.IsZero() && now.Sub(s.statsCachedAt) < statsCacheTTL {
		out := s.statsCached
		s.statsMu.Unlock()
		return out
	}
	s.statsMu.Unlock()

	liveNodes := 0
	var directory directoryStats
	seen := map[string]struct{}{}
	if s.nodes != nil {
		liveNodes = s.nodes.Count()
		directory.Sources.Advert = liveNodes
		directory.Sources.CoreScope = liveNodes
		addLiveDirectoryStats(s.nodes, &directory, seen, nowUnix())
	}
	importedNodes := 0
	if s.imported != nil {
		records := s.imported.Records()
		importedNodes = len(records)
		directory.Sources.Map = importedNodes
		addImportedDirectoryStats(records, &directory, seen, nowUnix())
	}
	out := statsResponse{
		Nodes: statsNodeCounts{
			Live:     liveNodes,
			Imported: importedNodes,
			Total:    liveNodes + importedNodes,
		},
		Directory: directory,
	}
	if s.db != nil {
		sqlite := s.db.Stats()
		out.SQLite = &sqlite
	}
	s.statsMu.Lock()
	s.statsCached = out
	s.statsCachedAt = now
	s.statsMu.Unlock()
	return out
}

func addLiveDirectoryStats(r *NodeRegistry, stats *directoryStats, seen map[string]struct{}, now int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for pubkey, n := range r.nodes {
		seen[pubkey] = struct{}{}
		addDirectoryNode(stats, byte(n.NodeType), n.LastAdvertAt, n.HasGPS && validCoords(n.Lat, n.Lon), n.Name, now)
	}
}

func addImportedDirectoryStats(records []*ImportedNode, stats *directoryStats, seen map[string]struct{}, now int64) {
	for _, n := range records {
		if n == nil {
			continue
		}
		if _, exists := seen[n.PublicKey]; exists {
			continue
		}
		seen[n.PublicKey] = struct{}{}
		addDirectoryNode(stats, byte(n.Type), n.lastAdvertUnix(), n.hasCoords(), n.AdvName, now)
	}
}

func addDirectoryNode(stats *directoryStats, typ byte, lastSeen int64, hasLocation bool, name string, now int64) {
	stats.Total++
	switch typ {
	case 1:
		stats.Types.Companion++
	case 2:
		stats.Types.Repeater++
	case 3:
		stats.Types.Room++
	case 4:
		stats.Types.Sensor++
	default:
		stats.Types.Unknown++
	}
	if lastSeen >= now-int64((24*time.Hour).Seconds()) {
		stats.Freshness.Last24h++
	}
	if lastSeen >= now-int64((7*24*time.Hour).Seconds()) {
		stats.Freshness.Last7d++
	}
	if lastSeen <= now-int64((30*24*time.Hour).Seconds()) {
		stats.Freshness.OlderThan30d++
	}
	if hasLocation {
		stats.Data.WithLocation++
	}
	if strings.TrimSpace(name) != "" {
		stats.Data.WithName++
	}
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.statsSnapshot()
	w.Header().Set("Cache-Control", "public, max-age=15")
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s.metrics == nil {
		http.NotFound(w, r)
		return
	}
	stats := s.statsSnapshot()
	s.metrics.updateStorageStats(stats.Nodes.Live, stats.Nodes.Imported, stats.SQLite)
	s.metrics.handler().ServeHTTP(w, r)
}

type analyzerDetail struct {
	Name           string            `json:"name"`
	URL            string            `json:"url"`
	Connected      bool              `json:"connected"`
	ConnectedSince int64             `json:"connectedSince"`
	LastError      string            `json:"lastError,omitempty"`
	PktPerMin      float64           `json:"pktPerMin"`
	UniquePackets  uint64            `json:"uniquePackets"`
	Observations   uint64            `json:"observations"`
	Observers      int               `json:"observers"`
	Nodes          int               `json:"nodes"`
	PayloadTypes   map[string]uint64 `json:"payloadTypes"`
	LastPacketAt   int64             `json:"lastPacketAt"`
}

type networkDetail struct {
	networkSummary
	PayloadTypes map[string]uint64 `json:"payloadTypes"`
	Analyzers    []analyzerDetail  `json:"analyzers"`
}

func (s *Server) summaryFor(ns *NetworkState, now int64) networkSummary {
	snap := ns.Counter.Snapshot(now)
	connected := 0
	for _, a := range ns.Analyzers {
		if ok, _, _ := a.status(); ok {
			connected++
		}
	}
	return networkSummary{
		ID:                 ns.ID,
		Name:               ns.Name,
		PktPerMin:          snap.PktPerMin,
		UniquePackets:      snap.UniquePackets,
		Observations:       snap.Observations,
		Observers:          snap.Observers,
		Nodes:              snap.Nodes,
		AnalyzersTotal:     len(ns.Analyzers),
		AnalyzersConnected: connected,
		LastPacketAt:       snap.LastPacketAt,
	}
}

func (s *Server) handleNetworks(w http.ResponseWriter, r *http.Request) {
	now := nowUnix()
	networks := s.store.NetworksSnapshot()
	out := make([]networkSummary, 0, len(networks))
	for _, ns := range networks {
		out = append(out, s.summaryFor(ns, now))
	}
	writeJSON(w, http.StatusOK, map[string]any{"networks": out})
}

func (s *Server) handleNetworkDetail(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/networks/")
	id = strings.Trim(id, "/")
	ns := s.store.Network(id)
	if ns == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown network"})
		return
	}
	now := nowUnix()
	netSnap := ns.Counter.Snapshot(now)

	analyzers := make([]analyzerDetail, 0, len(ns.Analyzers))
	for _, a := range ns.Analyzers {
		snap := a.Counter.Snapshot(now)
		connected, since, lastErr := a.status()
		analyzers = append(analyzers, analyzerDetail{
			Name:           a.Name,
			URL:            a.URL,
			Connected:      connected,
			ConnectedSince: since,
			LastError:      lastErr,
			PktPerMin:      snap.PktPerMin,
			UniquePackets:  snap.UniquePackets,
			Observations:   snap.Observations,
			Observers:      snap.Observers,
			Nodes:          snap.Nodes,
			PayloadTypes:   snap.PayloadTypes,
			LastPacketAt:   snap.LastPacketAt,
		})
	}

	writeJSON(w, http.StatusOK, networkDetail{
		networkSummary: s.summaryFor(ns, now),
		PayloadTypes:   netSnap.PayloadTypes,
		Analyzers:      analyzers,
	})
}

// handleNodes serves the global node registry overview. Each node carries the
// set of networks it has been heard on and its own rolling list of recent adverts.
func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"nodes": s.nodes.Snapshot(),
	})
}

// link endpoint defaults: 50 links returned by default, hard-capped at 200,
// sorted by recent activity descending.
const (
	defaultLinksLimit = 50
	maxLinksLimit     = 200
)

// linkNeighborView is the neighbor metadata embedded in a link, resolved through
// the global node registry (with an imported-directory fallback) so the frontend
// can render the link without a second request. Coordinates are omitted when the
// neighbor has no known GPS — such links list but cannot be drawn.
type linkNeighborView struct {
	PubKey   string  `json:"pubkey"`
	Name     string  `json:"name"`
	Type     byte    `json:"type"`
	TypeName string  `json:"typeName"`
	HasGPS   bool    `json:"hasGps"`
	Lat      float64 `json:"lat,omitempty"`
	Lon      float64 `json:"lon,omitempty"`
}

type linkView struct {
	Neighbor       linkNeighborView `json:"neighbor"`
	PacketCount    uint64           `json:"packetCount"`
	RecentActivity float64          `json:"recentActivity"`
	FirstSeen      int64            `json:"firstSeen"`
	LastSeen       int64            `json:"lastSeen"`
	Networks       []string         `json:"networks"`
	// Direction relative to the selected node: how many counted packets it sent to
	// vs received from this neighbor. Exposes asymmetric (one-way) links.
	SentByNode uint64            `json:"sentByNode"`
	RecvByNode uint64            `json:"recvByNode"`
	LastHash   string            `json:"lastHash,omitempty"` // content hash of the most recent packet on this link
	LastSNR    *float64          `json:"lastSnr,omitempty"`  // best-effort per-hop SNR (dB) from a TRACE, if any
	Sources    map[string]uint64 `json:"sources,omitempty"`  // counted events by route type (flood/direct/…)
	Geometry   *linkGeometryView `json:"geometry,omitempty"` // endpoint positions at observation, if known
}

// linkGeometryView is the link's drawable geometry from the selected node's
// frame: where each end sat when the link was observed, plus a moved flag that
// warns the current positions may differ (an endpoint relocated since).
type linkGeometryView struct {
	NodeLat     float64 `json:"nodeLat"`
	NodeLon     float64 `json:"nodeLon"`
	NeighborLat float64 `json:"neighborLat"`
	NeighborLon float64 `json:"neighborLon"`
	Moved       bool    `json:"moved"`
	Segments    int     `json:"segments"` // count of frozen historical position segments
}

// handleNodeSub routes the per-node resources:
//
//	GET /api/nodes/{pubkey}          node detail (overview + rolling adverts)
//	GET /api/nodes/{pubkey}/adverts  full advert history (paginated)
//	GET /api/nodes/{pubkey}/links    observed links
//	GET /api/nodes/{pubkey}/networks per-network advert activity
//	GET /api/nodes/{pubkey}/activity advert counts per UTC day
//	GET /api/nodes/{pubkey}/map      captured map.meshcore.io publishes
func (s *Server) handleNodeSub(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/nodes/")
	pubkey, sub, _ := strings.Cut(rest, "/")
	sub = strings.Trim(sub, "/")
	switch sub {
	case "":
		s.handleNodeDetail(w, r, pubkey)
	case "adverts":
		s.handleNodeAdverts(w, r, pubkey)
	case "links":
		s.handleNodeLinks(w, r, pubkey)
	case "networks":
		s.handleNodeNetworks(w, r, pubkey)
	case "activity":
		s.handleNodeActivity(w, r, pubkey)
	case "map":
		s.handleNodeMapPublishes(w, r, pubkey)
	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
	}
}

const (
	defaultActivityDays = 365
	maxActivityDays     = 730
)

// handleNodeActivity serves one node's daily advert counts for a GitHub-style
// activity heatmap:
//
//	GET /api/nodes/{pubkey}/activity?days=
func (s *Server) handleNodeActivity(w http.ResponseWriter, r *http.Request, rawPub string) {
	node, ok := normalizePub(rawPub)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid pubkey"})
		return
	}
	pubHex := hex.EncodeToString(node[:])

	days := atoiDefault(r.URL.Query().Get("days"), defaultActivityDays)
	if days <= 0 {
		days = defaultActivityDays
	}
	if days > maxActivityDays {
		days = maxActivityDays
	}

	now := nowUnix()
	today := (now / 86400) * 86400
	since := today - int64(days-1)*86400

	activity := []DailyAdvertStat{}
	if s.db != nil {
		rows, err := s.db.DailyAdvertStatsForNode(pubHex, since)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
			return
		}
		if rows != nil {
			activity = rows
		}
	}

	w.Header().Set("Cache-Control", "public, max-age=300")
	writeJSON(w, http.StatusOK, map[string]any{
		"node":     pubHex,
		"days":     days,
		"from":     since,
		"to":       today,
		"activity": activity,
	})
}

// handleNodeNetworks serves one node's per-network advert activity (count and
// first/last advert time per network), newest-active first:
//
//	GET /api/nodes/{pubkey}/networks
//
// Aggregated from the advert history table, so it needs the database; without it
// the response is an empty list (the directory then shows networks unenriched).
func (s *Server) handleNodeNetworks(w http.ResponseWriter, r *http.Request, rawPub string) {
	node, ok := normalizePub(rawPub)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid pubkey"})
		return
	}
	pubHex := hex.EncodeToString(node[:])

	stats := []NetworkAdvertStat{}
	if s.db != nil {
		rows, err := s.db.NetworkAdvertStatsForNode(pubHex)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
			return
		}
		if rows != nil {
			stats = rows
		}
	}
	w.Header().Set("Cache-Control", "public, max-age=15")
	writeJSON(w, http.StatusOK, map[string]any{
		"node":     pubHex,
		"networks": stats,
	})
}

// handleNodeMapPublishes serves the captured map.meshcore.io publish history for
// one node, newest publish first:
//
//	GET /api/nodes/{pubkey}/map
//
// Each entry is a distinct snapshot of the node's directory metadata we have
// mirrored over time. Served from the durable history table; without a database
// it falls back to the current in-memory record so the endpoint still answers.
func (s *Server) handleNodeMapPublishes(w http.ResponseWriter, r *http.Request, rawPub string) {
	node, ok := normalizePub(rawPub)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid pubkey"})
		return
	}
	pubHex := hex.EncodeToString(node[:])

	publishes := []ImportedSnapshot{}
	if s.db != nil {
		rows, err := s.db.ImportedNodeHistory(pubHex)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
			return
		}
		if rows != nil {
			publishes = rows
		}
	}
	// No durable history (db disabled, or first sync still pending): surface the
	// current mirrored record so the node still shows its directory presence.
	if len(publishes) == 0 && s.imported != nil {
		for _, in := range s.imported.ForPubKey(pubHex) {
			publishes = append(publishes, in.snapshot())
		}
	}

	w.Header().Set("Cache-Control", "public, max-age=60")
	writeJSON(w, http.StatusOK, map[string]any{
		"node":      pubHex,
		"publishes": publishes,
	})
}

// handleNodeDetail serves one node's overview row and its rolling latest-adverts
// list — the directory profile's primary fetch, avoiding the multi-MB /api/nodes
// download.
func (s *Server) handleNodeDetail(w http.ResponseWriter, r *http.Request, rawPub string) {
	node, ok := normalizePub(rawPub)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid pubkey"})
		return
	}
	pubHex := hex.EncodeToString(node[:])
	if view, found := s.nodes.GetView(pubHex); found {
		view.Source = "live"
		view.OnMap = s.imported != nil && s.imported.Has(pubHex)
		w.Header().Set("Cache-Control", "public, max-age=15")
		writeJSON(w, http.StatusOK, view)
		return
	}

	// Not observed by our analyzers: fall back to the mirrored directory so a
	// map-only node still has a profile (built from its imported record).
	if s.imported != nil {
		if recs := s.imported.ForPubKey(pubHex); len(recs) > 0 {
			w.Header().Set("Cache-Control", "public, max-age=15")
			writeJSON(w, http.StatusOK, importedNodeView(recs[0]))
			return
		}
	}
	writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown node"})
}

// importedNodeView builds a node profile from a directory-only record. It has no
// observed adverts, networks, or links — those endpoints return empty for it —
// and is tagged source "map" so the UI can explain the data is mirrored.
func importedNodeView(n *ImportedNode) NodeView {
	t := byte(n.Type)
	v := NodeView{
		PubKey:        n.PublicKey,
		Name:          n.AdvName,
		Type:          t,
		TypeName:      nodeTypeName(t),
		LastAdvertAt:  n.lastAdvertUnix(),
		Networks:      []string{},
		LatestAdverts: []AdvertView{},
		Source:        "map",
		OnMap:         true,
	}
	if n.hasCoords() {
		v.HasGPS = true
		v.Lat = n.AdvLat
		v.Lon = n.AdvLon
	}
	return v
}

// advert history endpoint defaults: 50 adverts per page, hard-capped at 500.
const (
	defaultAdvertsLimit = 50
	maxAdvertsLimit     = 500
)

// handleNodeAdverts serves one node's full advert history from the append-only
// history table, newest first:
//
//	GET /api/nodes/{pubkey}/adverts?limit=&before=
//
// before is the keyset cursor returned as nextBefore by the previous page (omit
// for the newest page). When the database is disabled the in-memory rolling list
// is served instead, so the endpoint still works (just without deep history).
func (s *Server) handleNodeAdverts(w http.ResponseWriter, r *http.Request, rawPub string) {
	node, ok := normalizePub(rawPub)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid pubkey"})
		return
	}
	pubHex := hex.EncodeToString(node[:])

	qv := r.URL.Query()
	limit := atoiDefault(qv.Get("limit"), defaultAdvertsLimit)
	if limit <= 0 {
		limit = defaultAdvertsLimit
	}
	if limit > maxAdvertsLimit {
		limit = maxAdvertsLimit
	}
	before := int64(atoiDefault(qv.Get("before"), 0))

	// No database: fall back to the in-memory rolling list (no pagination).
	if s.db == nil {
		view, found := s.nodes.GetView(pubHex)
		if !found {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown node"})
			return
		}
		adverts := view.LatestAdverts
		if len(adverts) > limit {
			adverts = adverts[:limit]
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"node":     pubHex,
			"adverts":  adverts,
			"returned": len(adverts),
			"hasMore":  false,
		})
		return
	}

	rows, nextBefore, err := s.db.AdvertsForNode(pubHex, limit, before)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
		return
	}
	adverts := advertViews(rows)
	hasMore := len(rows) == limit
	out := map[string]any{
		"node":     pubHex,
		"adverts":  adverts,
		"returned": len(adverts),
		"hasMore":  hasMore,
	}
	if hasMore {
		out["nextBefore"] = nextBefore
	}
	w.Header().Set("Cache-Control", "public, max-age=15")
	writeJSON(w, http.StatusOK, out)
}

// search endpoint defaults: 50 results returned by default, hard-capped at 200.
const (
	defaultSearchLimit = 50
	maxSearchLimit     = 200
)

type searchOptionValue struct {
	Value string `json:"value"`
	Label string `json:"label"`
}

type searchOptionCommand struct {
	Key         string              `json:"key"`
	Label       string              `json:"label"`
	Description string              `json:"description,omitempty"`
	Values      []searchOptionValue `json:"values,omitempty"`
	Placeholder string              `json:"placeholder,omitempty"`
}

func (s *Server) searchOptions() []searchOptionCommand {
	countries := map[string]bool{}
	regions := map[string]bool{}
	if s.store != nil {
		for _, ns := range s.store.NetworksSnapshot() {
			for _, cc := range ns.Countries {
				countries[cc] = true
			}
			for _, r := range ns.Regions {
				regions[r] = true
			}
		}
	}
	countryValues := make([]searchOptionValue, 0, len(countries))
	for cc := range countries {
		countryValues = append(countryValues, searchOptionValue{Value: cc, Label: cc})
	}
	sort.Slice(countryValues, func(i, j int) bool { return countryValues[i].Value < countryValues[j].Value })
	regionValues := make([]searchOptionValue, 0, len(regions))
	for r := range regions {
		regionValues = append(regionValues, searchOptionValue{Value: r, Label: r})
	}
	sort.Slice(regionValues, func(i, j int) bool { return regionValues[i].Value < regionValues[j].Value })

	return []searchOptionCommand{
		{Key: "type", Label: "Type", Values: []searchOptionValue{{"repeater", "Repeater"}, {"companion", "Companion"}, {"room", "Room"}}},
		{Key: "country", Label: "Country", Values: countryValues, Placeholder: "CZ"},
		{Key: "region", Label: "Region", Values: regionValues, Placeholder: "EU868"},
		{Key: "seen", Label: "Seen", Values: []searchOptionValue{{"<24h", "Last 24 hours"}, {"<7d", "Last 7 days"}, {">30d", "Older than 30 days"}}},
		{Key: "has", Label: "Has", Values: []searchOptionValue{{"location", "Location"}, {"name", "Name"}}},
		{Key: "source", Label: "Source", Values: []searchOptionValue{{"advert", "Advert"}, {"map", "Map"}, {"corescope", "CoreScope"}}},
		{Key: "near", Label: "Near", Placeholder: "50.0755,14.4378"},
		{Key: "radius", Label: "Radius", Placeholder: "25"},
		{Key: "sort", Label: "Sort", Values: []searchOptionValue{{"recent", "Recent"}, {"name", "Name"}, {"distance", "Distance"}}},
	}
}

func (s *Server) handleSearchOptions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "public, max-age=300")
	writeJSON(w, http.StatusOK, map[string]any{"commands": s.searchOptions()})
}

func addSet(dst map[string]bool, values []string) map[string]bool {
	for _, v := range values {
		for _, part := range strings.Split(v, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if dst == nil {
				dst = map[string]bool{}
			}
			dst[part] = true
		}
	}
	return dst
}

func (s *Server) expandNetworkMetadata(p *MapParams) {
	if len(p.Countries) == 0 && len(p.Regions) == 0 {
		return
	}
	if s.store == nil {
		p.Networks = map[string]bool{"__no_matching_network__": true}
		return
	}
	metaNetworks := map[string]bool{}
	for _, ns := range s.store.NetworksSnapshot() {
		countryOK := len(p.Countries) == 0
		for _, cc := range ns.Countries {
			if p.Countries[cc] {
				countryOK = true
				break
			}
		}
		regionOK := len(p.Regions) == 0
		for _, r := range ns.Regions {
			if p.Regions[r] {
				regionOK = true
				break
			}
		}
		if countryOK && regionOK {
			metaNetworks[ns.ID] = true
		}
	}
	if p.Networks == nil {
		p.Networks = metaNetworks
		return
	}
	for id := range p.Networks {
		if !metaNetworks[id] {
			delete(p.Networks, id)
		}
	}
}

func (s *Server) supportedSearchMeta() (map[string]bool, map[string]bool) {
	countries, regions := map[string]bool{}, map[string]bool{}
	if s.store == nil {
		return countries, regions
	}
	for _, ns := range s.store.NetworksSnapshot() {
		for _, cc := range ns.Countries {
			countries[cc] = true
		}
		for _, r := range ns.Regions {
			regions[r] = true
		}
	}
	return countries, regions
}

func parseSearchParams(s *Server, r *http.Request) (MapParams, int, string) {
	qv := r.URL.Query()
	p := MapParams{
		Types:    parseByteSet(qv.Get("types")),
		Networks: parseStringSet(qv.Get("networks")),
		Q:        strings.TrimSpace(qv.Get("q")),
		Sort:     strings.TrimSpace(qv.Get("sort")),
	}

	for _, typ := range qv["type"] {
		for _, v := range strings.Split(typ, ",") {
			switch strings.ToLower(strings.TrimSpace(v)) {
			case "":
			case "companion", "chat":
				if p.Types == nil {
					p.Types = map[byte]bool{}
				}
				p.Types[1] = true
			case "repeater":
				if p.Types == nil {
					p.Types = map[byte]bool{}
				}
				p.Types[2] = true
			case "room":
				if p.Types == nil {
					p.Types = map[byte]bool{}
				}
				p.Types[3] = true
			default:
				return p, 0, "unsupported type"
			}
		}
	}

	p.Countries = addSet(p.Countries, qv["country"])
	supportedCountries, supportedRegions := s.supportedSearchMeta()
	for cc := range p.Countries {
		if len(cc) != 2 || strings.ToUpper(cc) != cc {
			return p, 0, "unsupported country"
		}
		if len(supportedCountries) > 0 && !supportedCountries[cc] {
			return p, 0, "unsupported country"
		}
	}
	p.Regions = addSet(p.Regions, qv["region"])
	for r := range p.Regions {
		if r == "" || strings.ToUpper(r) != r {
			return p, 0, "unsupported region"
		}
		if len(supportedRegions) > 0 && !supportedRegions[r] {
			return p, 0, "unsupported region"
		}
	}
	s.expandNetworkMetadata(&p)

	if since := qv.Get("since"); since != "" {
		p.Since = int64(atoiDefault(since, 0))
	} else if d, ok := parseActive(qv.Get("active")); ok {
		p.Since = nowUnix() - int64(d.Seconds())
	}
	for _, seen := range qv["seen"] {
		switch strings.TrimSpace(seen) {
		case "", "all":
		case "<24h":
			p.Since = nowUnix() - int64((24 * time.Hour).Seconds())
		case "<7d":
			p.Since = nowUnix() - int64((7 * 24 * time.Hour).Seconds())
		case ">30d":
			p.OlderThan = nowUnix() - int64((30 * 24 * time.Hour).Seconds())
		default:
			return p, 0, "unsupported seen"
		}
	}

	for _, has := range qv["has"] {
		switch strings.TrimSpace(has) {
		case "":
		case "location":
			v := true
			p.HasLocation = &v
		case "name":
			v := true
			p.HasName = &v
		default:
			return p, 0, "unsupported has"
		}
	}

	p.Sources = addSet(p.Sources, qv["source"])
	for src := range p.Sources {
		if src != "advert" && src != "map" && src != "corescope" {
			return p, 0, "unsupported source"
		}
	}

	if near := strings.TrimSpace(qv.Get("near")); near != "" {
		parts := strings.Split(near, ",")
		if len(parts) != 2 {
			return p, 0, "invalid near"
		}
		lat, err1 := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
		lon, err2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		if err1 != nil || err2 != nil || lat < -90 || lat > 90 || lon < -180 || lon > 180 {
			return p, 0, "invalid near"
		}
		radius, err := strconv.ParseFloat(strings.TrimSpace(qv.Get("radius")), 64)
		if err != nil || radius <= 0 || radius > 20000 {
			return p, 0, "invalid radius"
		}
		p.NearLat, p.NearLon, p.RadiusKM, p.HasNear = lat, lon, radius, true
	}
	switch p.Sort {
	case "", "recent", "name":
	case "distance":
		if !p.HasNear {
			return p, 0, "sort distance requires near"
		}
	default:
		return p, 0, "unsupported sort"
	}

	limit := atoiDefault(qv.Get("limit"), defaultSearchLimit)
	if limit <= 0 {
		limit = defaultSearchLimit
	}
	if limit > maxSearchLimit {
		limit = maxSearchLimit
	}
	return p, limit, ""
}

// handleSearch serves the directory's main query against the node registry:
//
//	GET /api/search?q=&types=&networks=&active=&since=&limit=
//
// Unlike the snapshot endpoint it includes nodes without GPS, so every observed node is
// findable. Results are ranked by relevance (exact/prefix name, then pubkey
// prefix, then substring) and recency, and carry no per-node advert list to keep
// the payload small.
func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	p, limit, bad := parseSearchParams(s, r)
	if bad != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": bad})
		return
	}

	started := time.Now()
	results, total, capped := s.mergedSearch(p, limit)
	computeMS := float64(time.Since(started).Microseconds()) / 1000
	w.Header().Set("Cache-Control", "public, max-age=15")
	writeJSON(w, http.StatusOK, map[string]any{
		"results":   results,
		"returned":  len(results),
		"total":     total,
		"capped":    capped,
		"computeMs": computeMS,
	})
}

// mergedSearch ranks live nodes and the mirrored map.meshcore.io directory
// together so directory-only nodes are findable. Live nodes win on duplicate
// public keys. Results are ranked by relevance then recency (matching the live
// registry's own ordering) and capped to limit; total is the full match count.
func (s *Server) mergedSearch(p MapParams, limit int) (results []SearchResult, total int, capped bool) {
	q := strings.ToLower(strings.TrimSpace(p.Q))

	// All matching live nodes, already source-tagged. Passing limit 0 keeps every
	// hit so the merge ranks across both sources before truncating.
	live, _, _ := s.nodes.Search(p, 0)

	type scored struct {
		r    SearchResult
		rank int
	}
	all := make([]scored, 0, len(live))
	seen := make(map[string]bool, len(live))
	for _, r := range live {
		if p.HasNear && r.HasGPS {
			r.DistanceKM = haversineKM(p.NearLat, p.NearLon, r.Lat, r.Lon)
		}
		seen[r.PubKey] = true
		all = append(all, scored{r, rankMatch(r.Name, r.PubKey, q)})
	}

	if s.imported != nil {
		for _, in := range s.imported.Records() {
			if seen[in.PublicKey] || !p.matchesImported(in) {
				continue
			}
			r := importedSearchResult(in)
			if p.HasNear && r.HasGPS {
				r.DistanceKM = haversineKM(p.NearLat, p.NearLon, r.Lat, r.Lon)
			}
			seen[in.PublicKey] = true
			all = append(all, scored{r, rankMatch(in.AdvName, in.PublicKey, q)})
		}
	}

	sort.Slice(all, func(i, j int) bool {
		switch p.Sort {
		case "name":
			ni, nj := strings.ToLower(all[i].r.Name), strings.ToLower(all[j].r.Name)
			if ni != nj {
				return ni < nj
			}
			return all[i].r.PubKey < all[j].r.PubKey
		case "distance":
			if all[i].r.DistanceKM != all[j].r.DistanceKM {
				return all[i].r.DistanceKM < all[j].r.DistanceKM
			}
			if all[i].rank != all[j].rank {
				return all[i].rank < all[j].rank
			}
			return all[i].r.PubKey < all[j].r.PubKey
		}
		if all[i].rank != all[j].rank {
			return all[i].rank < all[j].rank
		}
		if all[i].r.LastAdvertAt != all[j].r.LastAdvertAt {
			return all[i].r.LastAdvertAt > all[j].r.LastAdvertAt
		}
		return all[i].r.PubKey < all[j].r.PubKey
	})

	total = len(all)
	if limit > 0 && len(all) > limit {
		all = all[:limit]
		capped = true
	}
	results = make([]SearchResult, 0, len(all))
	for _, s := range all {
		results = append(results, s.r)
	}
	return results, total, capped
}

// importedSearchResult renders a directory-only node as a search hit. It carries
// no networks or advert count (the directory tracks neither) and is tagged
// source "map" so the UI can flag it.
func importedSearchResult(n *ImportedNode) SearchResult {
	t := byte(n.Type)
	r := SearchResult{
		PubKey:       n.PublicKey,
		Name:         n.AdvName,
		Type:         t,
		TypeName:     nodeTypeName(t),
		LastAdvertAt: n.lastAdvertUnix(),
		Networks:     []string{},
		Source:       "map",
	}
	if n.hasCoords() {
		r.HasGPS = true
		r.Lat = n.AdvLat
		r.Lon = n.AdvLon
	}
	return r
}

// handleNodeLinks serves the observed links for one node:
//
//	GET /api/nodes/{pubkey}/links?limit=&active=&networks=
//
// Only links with the selected node as an endpoint are returned (never the global
// topology). The network filter narrows which links are included but never changes
// the globally-deduplicated packet count. Neighbor metadata is resolved here so
// the frontend needs no follow-up request.
func (s *Server) handleNodeLinks(w http.ResponseWriter, r *http.Request, rawPub string) {
	node, ok := normalizePub(rawPub)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid pubkey"})
		return
	}
	pubHex := hex.EncodeToString(node[:])

	qv := r.URL.Query()
	limit := atoiDefault(qv.Get("limit"), defaultLinksLimit)
	if limit <= 0 {
		limit = defaultLinksLimit
	}
	if limit > maxLinksLimit {
		limit = maxLinksLimit
	}
	netFilter := parseStringSet(qv.Get("networks"))
	var since int64
	if d, ok := parseActive(qv.Get("active")); ok {
		since = nowUnix() - int64(d.Seconds())
	}

	now := nowUnix()
	all := s.links.LinksForNode(node, now)

	// Apply the network and activity filters. The network filter only includes or
	// excludes whole links; it does not touch packetCount.
	filtered := all[:0:0]
	for _, l := range all {
		if since > 0 && l.LastSeen < since {
			continue
		}
		if len(netFilter) > 0 && !anyInSet(l.Networks, netFilter) {
			continue
		}
		filtered = append(filtered, l)
	}

	sortNeighborsByActivity(filtered)
	total := len(filtered)
	capped := false
	if len(filtered) > limit {
		filtered = filtered[:limit]
		capped = true
	}

	var imported []*ImportedNode
	if s.imported != nil {
		imported = s.imported.Records()
	}

	views := make([]linkView, 0, len(filtered))
	for _, l := range filtered {
		v := linkView{
			Neighbor:       s.neighborView(l.Neighbor, imported),
			PacketCount:    l.PacketCount,
			RecentActivity: round2(l.RecentActivity),
			FirstSeen:      l.FirstSeen,
			LastSeen:       l.LastSeen,
			Networks:       l.Networks,
			SentByNode:     l.SentByNode,
			RecvByNode:     l.RecvByNode,
			LastHash:       l.LastHash,
			Sources:        l.Sources,
		}
		if l.HasSNR {
			snr := round2(l.LastSNR)
			v.LastSNR = &snr
		}
		if l.HasPos {
			v.Geometry = &linkGeometryView{
				NodeLat: l.NodeLat, NodeLon: l.NodeLon,
				NeighborLat: l.NeighborLat, NeighborLon: l.NeighborLon,
				Moved: l.Moved, Segments: l.SegmentCount,
			}
		}
		views = append(views, v)
	}

	w.Header().Set("Cache-Control", "public, max-age=15")
	writeJSON(w, http.StatusOK, map[string]any{
		"node":     pubHex,
		"links":    views,
		"returned": len(views),
		"total":    total,
		"capped":   capped,
	})
}

// neighborView resolves a neighbor's display metadata: live node registry first,
// then the imported directory (which may enrich identity but never creates links).
// A neighbor with no known data still returns, flagged non-drawable (HasGPS false).
func (s *Server) neighborView(pubkey string, imported []*ImportedNode) linkNeighborView {
	if n, ok := s.nodes.Lookup(pubkey); ok {
		return linkNeighborView{
			PubKey:   n.PubKey,
			Name:     n.Name,
			Type:     n.NodeType,
			TypeName: nodeTypeName(n.NodeType),
			HasGPS:   n.HasGPS,
			Lat:      n.Lat,
			Lon:      n.Lon,
		}
	}
	for _, in := range imported {
		if in.PublicKey == pubkey {
			t := byte(in.Type)
			v := linkNeighborView{
				PubKey:   pubkey,
				Name:     in.AdvName,
				Type:     t,
				TypeName: nodeTypeName(t),
			}
			if in.hasCoords() {
				v.HasGPS = true
				v.Lat = in.AdvLat
				v.Lon = in.AdvLon
			}
			return v
		}
	}
	return linkNeighborView{PubKey: pubkey, TypeName: nodeTypeName(0)}
}

// anyInSet reports whether any value is present in the set.
func anyInSet(values []string, set map[string]bool) bool {
	for _, v := range values {
		if set[v] {
			return true
		}
	}
	return false
}

// routeHopView is one leg of a computed route, ready for JSON. The endpoint
// pubkeys are implied by the surrounding nodes list (hop i joins nodes[i] and
// nodes[i+1]), so only the link's own stats live here.
type routeHopView struct {
	PacketCount    uint64   `json:"packetCount"`
	RecentActivity float64  `json:"recentActivity"`
	FirstSeen      int64    `json:"firstSeen"`
	LastSeen       int64    `json:"lastSeen"`
	Networks       []string `json:"networks"`
}

// handleRoute serves a best-effort path between two nodes over the observed-link
// graph:
//
//	GET /api/route?from={pubkey}&to={pubkey}&active=&networks=
//
// The path is the lowest-cost route where each hop is weighted by how recent and
// busy that link is (see route.go). active/networks narrow the graph exactly like
// the links endpoint. When the two nodes are not connected through the filtered
// graph, found is false and nodes/hops are empty. Each node carries the same
// metadata shape as a link neighbor, so the frontend can draw the polyline and
// label each hop without follow-up requests.
func (s *Server) handleRoute(w http.ResponseWriter, r *http.Request) {
	qv := r.URL.Query()
	from, okFrom := normalizePub(qv.Get("from"))
	to, okTo := normalizePub(qv.Get("to"))
	if !okFrom || !okTo {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid from/to pubkey"})
		return
	}

	netFilter := parseStringSet(qv.Get("networks"))
	var since int64
	if d, ok := parseActive(qv.Get("active")); ok {
		since = nowUnix() - int64(d.Seconds())
	}

	now := nowUnix()
	res := s.links.RouteBetween(from, to, now, since, netFilter)

	var imported []*ImportedNode
	if s.imported != nil {
		imported = s.imported.Records()
	}

	nodes := make([]linkNeighborView, 0, len(res.Nodes))
	for _, pk := range res.Nodes {
		nodes = append(nodes, s.neighborView(pk, imported))
	}
	hops := make([]routeHopView, 0, len(res.Hops))
	for _, h := range res.Hops {
		hops = append(hops, routeHopView{
			PacketCount:    h.PacketCount,
			RecentActivity: round2(h.RecentActivity),
			FirstSeen:      h.FirstSeen,
			LastSeen:       h.LastSeen,
			Networks:       h.Networks,
		})
	}

	w.Header().Set("Cache-Control", "public, max-age=15")
	writeJSON(w, http.StatusOK, map[string]any{
		"from":  hex.EncodeToString(from[:]),
		"to":    hex.EncodeToString(to[:]),
		"found": res.Found,
		"nodes": nodes,
		"hops":  hops,
	})
}

// handleObservers serves the global observer activity table, most recently
// active first.
func (s *Server) handleObservers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"observers": s.observers.Snapshot(),
	})
}

// flaggedNodeView is one entry in the /api/flags response: the identity and
// location a consumer needs to act on a flag, without the heavy advert history.
type flaggedNodeView struct {
	PubKey       string   `json:"pubkey"`
	Name         string   `json:"name"`
	Type         byte     `json:"type"`
	TypeName     string   `json:"typeName"`
	Lat          float64  `json:"lat,omitempty"`
	Lon          float64  `json:"lon,omitempty"`
	Networks     []string `json:"networks"`
	Flags        []string `json:"flags"`
	LastAdvertAt int64    `json:"lastAdvertAt"`
}

// handleFlags lists every node currently carrying a flag, newest scan first.
//
//	GET /api/flags
func (s *Server) handleFlags(w http.ResponseWriter, r *http.Request) {
	recs := s.nodes.FlaggedNodes()
	nodes := make([]flaggedNodeView, 0, len(recs))
	for i := range recs {
		n := &recs[i]
		nodes = append(nodes, flaggedNodeView{
			PubKey:       n.PubKey,
			Name:         n.Name,
			Type:         n.NodeType,
			TypeName:     nodeTypeName(n.NodeType),
			Lat:          n.Lat,
			Lon:          n.Lon,
			Networks:     n.Networks,
			Flags:        n.Flags,
			LastAdvertAt: n.LastAdvertAt,
		})
	}
	resp := map[string]any{
		"count": len(nodes),
		"nodes": nodes,
	}
	if s.flagger != nil {
		resp["thresholdKm"] = s.flagger.ThresholdKM()
		if last := s.flagger.LastScan(); !last.IsZero() {
			resp["lastScanAt"] = last.UTC().Format(time.RFC3339)
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	now := nowUnix()
	analyzers, connected := 0, 0
	networks := s.store.NetworksSnapshot()
	for _, ns := range networks {
		for _, a := range ns.Analyzers {
			analyzers++
			if ok, _, _ := a.status(); ok {
				connected++
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                 true,
		"networks":           len(networks),
		"analyzers":          analyzers,
		"analyzersConnected": connected,
		"time":               now,
	})
}
