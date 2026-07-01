package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	_ "modernc.org/sqlite"
)

// DB persists per-scope counter snapshots to a SQLite file (pure-Go driver, no
// cgo) so cumulative totals and the node/observer gauges survive restarts. One
// row per scope; the periodic flush upserts every scope inside one transaction.
type DB struct {
	db        *sql.DB
	links     *sql.DB
	write     sync.Mutex
	linkWrite sync.Mutex
	stats     atomicStats
}

type atomicStats struct {
	nodes               atomic.Int64
	importedNodes       atomic.Int64
	adverts             atomic.Int64
	importedNodeHistory atomic.Int64
}

const counterSchema = `
CREATE TABLE IF NOT EXISTS counters (
	scope          TEXT PRIMARY KEY,
	observations   INTEGER NOT NULL DEFAULT 0,
	unique_packets INTEGER NOT NULL DEFAULT 0,
	last_packet_at INTEGER NOT NULL DEFAULT 0,
	payload_types  TEXT    NOT NULL DEFAULT '{}',
	observers      TEXT    NOT NULL DEFAULT '{}',
	nodes          TEXT    NOT NULL DEFAULT '{}',
	updated_at     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS nodes (
	pubkey          TEXT PRIMARY KEY,
	name            TEXT    NOT NULL DEFAULT '',
	node_type       INTEGER NOT NULL DEFAULT 0,
	has_gps         INTEGER NOT NULL DEFAULT 0,
	lat             REAL    NOT NULL DEFAULT 0,
	lon             REAL    NOT NULL DEFAULT 0,
	first_advert_at INTEGER NOT NULL DEFAULT 0,
	last_advert_at  INTEGER NOT NULL DEFAULT 0,
	advert_count    INTEGER NOT NULL DEFAULT 0,
	networks        TEXT    NOT NULL DEFAULT '[]',
	observer_id     TEXT    NOT NULL DEFAULT '',
	observer_name   TEXT    NOT NULL DEFAULT '',
	updated_at      INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS adverts (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	hash          TEXT    NOT NULL DEFAULT '',
	raw_hex       TEXT    NOT NULL DEFAULT '',
	pubkey        TEXT    NOT NULL,
	name          TEXT    NOT NULL DEFAULT '',
	node_type     INTEGER NOT NULL DEFAULT 0,
	has_gps       INTEGER NOT NULL DEFAULT 0,
	lat           REAL    NOT NULL DEFAULT 0,
	lon           REAL    NOT NULL DEFAULT 0,
	advert_time   INTEGER NOT NULL DEFAULT 0,
	received_at   INTEGER NOT NULL DEFAULT 0,
	network_id    TEXT    NOT NULL DEFAULT '',
	analyzer_name TEXT    NOT NULL DEFAULT '',
	observer_id   TEXT    NOT NULL DEFAULT '',
	observer_name TEXT    NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_adverts_pubkey ON adverts(pubkey, id);

CREATE TABLE IF NOT EXISTS observers (
	observer_id  TEXT PRIMARY KEY,
	name         TEXT    NOT NULL DEFAULT '',
	first_seen   INTEGER NOT NULL DEFAULT 0,
	last_seen    INTEGER NOT NULL DEFAULT 0,
	observations INTEGER NOT NULL DEFAULT 0,
	networks     TEXT    NOT NULL DEFAULT '[]',
	updated_at   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS imported_nodes (
	public_key      TEXT PRIMARY KEY,
	type            INTEGER NOT NULL DEFAULT 0,
	adv_name        TEXT    NOT NULL DEFAULT '',
	last_advert     TEXT    NOT NULL DEFAULT '',
	last_advert_at  INTEGER NOT NULL DEFAULT 0,
	adv_lat         REAL    NOT NULL DEFAULT 0,
	adv_lon         REAL    NOT NULL DEFAULT 0,
	inserted_date   TEXT    NOT NULL DEFAULT '',
	updated_date    TEXT    NOT NULL DEFAULT '',
	params          TEXT    NOT NULL DEFAULT '{}',
	link            TEXT    NOT NULL DEFAULT '',
	source          TEXT    NOT NULL DEFAULT '',
	inserted_by     TEXT    NOT NULL DEFAULT '',
	updated_by      TEXT    NOT NULL DEFAULT '',
	synced_at       INTEGER NOT NULL DEFAULT 0
);

-- Append-only history of map.meshcore.io "publishes": every distinct snapshot of
-- a node's published metadata we have ever mirrored. A row is keyed by
-- (public_key, sig) where sig is a content hash over the publish-relevant fields
-- (everything except the frequently-changing last_advert), so re-syncs that only
-- bump last_advert don't create new rows. first_captured_at is when we first saw
-- that snapshot; last_captured_at is the most recent sync that still matched it.
CREATE TABLE IF NOT EXISTS imported_node_history (
	id                INTEGER PRIMARY KEY AUTOINCREMENT,
	public_key        TEXT    NOT NULL,
	sig               TEXT    NOT NULL,
	type              INTEGER NOT NULL DEFAULT 0,
	adv_name          TEXT    NOT NULL DEFAULT '',
	last_advert       TEXT    NOT NULL DEFAULT '',
	last_advert_at    INTEGER NOT NULL DEFAULT 0,
	adv_lat           REAL    NOT NULL DEFAULT 0,
	adv_lon           REAL NOT NULL DEFAULT 0,
	inserted_date     TEXT    NOT NULL DEFAULT '',
	updated_date      TEXT    NOT NULL DEFAULT '',
	params            TEXT    NOT NULL DEFAULT '{}',
	link              TEXT    NOT NULL DEFAULT '',
	source            TEXT    NOT NULL DEFAULT '',
	inserted_by       TEXT    NOT NULL DEFAULT '',
	updated_by        TEXT    NOT NULL DEFAULT '',
	first_captured_at INTEGER NOT NULL DEFAULT 0,
	last_captured_at  INTEGER NOT NULL DEFAULT 0,
	UNIQUE(public_key, sig)
);
CREATE INDEX IF NOT EXISTS idx_imported_history_pubkey ON imported_node_history(public_key, first_captured_at);`

const linkSchema = `
CREATE TABLE IF NOT EXISTS links (
	node_a           TEXT    NOT NULL,
	node_b           TEXT    NOT NULL,
	packet_count     INTEGER NOT NULL DEFAULT 0,
	first_seen       INTEGER NOT NULL DEFAULT 0,
	last_seen        INTEGER NOT NULL DEFAULT 0,
	activity_score   REAL    NOT NULL DEFAULT 0,
	score_updated_at INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (node_a, node_b)
);
-- The PK covers lookups by node_a; this index covers lookups by node_b, so all
-- links touching a selected public key (either endpoint) are found cheaply.
CREATE INDEX IF NOT EXISTS idx_links_node_b ON links(node_b);

CREATE TABLE IF NOT EXISTS link_networks (
	node_a     TEXT    NOT NULL,
	node_b     TEXT    NOT NULL,
	network_id TEXT    NOT NULL,
	first_seen INTEGER NOT NULL DEFAULT 0,
	last_seen  INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (node_a, node_b, network_id)
);`

// OpenDB opens (creating if needed) the core SQLite store plus the separate link
// graph store. WAL mode and a busy timeout keep periodic writers from tripping
// over readers.
func OpenDB(path, linksPath string) (*DB, error) {
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	sdb, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	if _, err := sdb.Exec(counterSchema); err != nil {
		_ = sdb.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	if err := ensureColumn(sdb, "adverts", "hash", "TEXT NOT NULL DEFAULT ''"); err != nil {
		_ = sdb.Close()
		return nil, fmt.Errorf("migrate adverts.hash: %w", err)
	}
	if err := ensureColumn(sdb, "adverts", "raw_hex", "TEXT NOT NULL DEFAULT ''"); err != nil {
		_ = sdb.Close()
		return nil, fmt.Errorf("migrate adverts.raw_hex: %w", err)
	}
	if err := ensureColumn(sdb, "adverts", "analyzer_name", "TEXT NOT NULL DEFAULT ''"); err != nil {
		_ = sdb.Close()
		return nil, fmt.Errorf("migrate adverts.analyzer_name: %w", err)
	}
	if err := ensureColumn(sdb, "nodes", "flags", "TEXT NOT NULL DEFAULT '[]'"); err != nil {
		_ = sdb.Close()
		return nil, fmt.Errorf("migrate nodes.flags: %w", err)
	}
	linksDB, err := sql.Open("sqlite", "file:"+linksPath+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		_ = sdb.Close()
		return nil, err
	}
	if _, err := linksDB.Exec(linkSchema); err != nil {
		_ = sdb.Close()
		_ = linksDB.Close()
		return nil, fmt.Errorf("init link schema: %w", err)
	}
	for _, m := range []struct{ col, decl string }{
		{"last_hash_ab", "TEXT NOT NULL DEFAULT ''"},
		{"last_hash_ba", "TEXT NOT NULL DEFAULT ''"},
		{"dir_ab", "INTEGER NOT NULL DEFAULT 0"},
		{"dir_ba", "INTEGER NOT NULL DEFAULT 0"},
		{"last_snr", "REAL NOT NULL DEFAULT 0"},
		{"has_snr", "INTEGER NOT NULL DEFAULT 0"},
		{"snrs_ab", "TEXT NOT NULL DEFAULT '[]'"},
		{"snrs_ba", "TEXT NOT NULL DEFAULT '[]'"},
		{"geo", "TEXT NOT NULL DEFAULT ''"},
		{"sources", "TEXT NOT NULL DEFAULT '{}'"},
	} {
		if err := ensureColumn(linksDB, "links", m.col, m.decl); err != nil {
			_ = sdb.Close()
			_ = linksDB.Close()
			return nil, fmt.Errorf("migrate links.%s: %w", m.col, err)
		}
	}
	if err := dropColumnIfExists(linksDB, "links", "last_hash"); err != nil {
		_ = sdb.Close()
		_ = linksDB.Close()
		return nil, fmt.Errorf("migrate links.last_hash: %w", err)
	}
	d := &DB{db: sdb, links: linksDB}
	if err := d.initStats(); err != nil {
		_ = sdb.Close()
		_ = linksDB.Close()
		return nil, fmt.Errorf("init stats: %w", err)
	}
	return d, nil
}

func (d *DB) Close() error {
	err := d.db.Close()
	if linkErr := d.links.Close(); err == nil {
		err = linkErr
	}
	return err
}

// SQLiteStats is a lightweight snapshot of current durable table sizes.
type SQLiteStats struct {
	Nodes               int64 `json:"nodes"`
	ImportedNodes       int64 `json:"importedNodes"`
	Adverts             int64 `json:"adverts"`
	ImportedNodeHistory int64 `json:"importedNodeHistory"`
}

func (d *DB) initStats() error {
	for _, c := range []struct {
		query string
		dst   *atomic.Int64
	}{
		{`SELECT COUNT(*) FROM nodes`, &d.stats.nodes},
		{`SELECT COUNT(*) FROM imported_nodes`, &d.stats.importedNodes},
		{`SELECT COALESCE(MAX(id), 0) FROM adverts`, &d.stats.adverts},
		{`SELECT COALESCE(MAX(id), 0) FROM imported_node_history`, &d.stats.importedNodeHistory},
	} {
		var n int64
		if err := d.db.QueryRow(c.query).Scan(&n); err != nil {
			return err
		}
		c.dst.Store(n)
	}
	return nil
}

func ensureColumn(db *sql.DB, table, column, decl string) error {
	ok, err := columnExists(db, table, column)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	_, err = db.Exec(`ALTER TABLE ` + table + ` ADD COLUMN ` + column + ` ` + decl)
	return err
}

func dropColumnIfExists(db *sql.DB, table, column string) error {
	ok, err := columnExists(db, table, column)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	_, err = db.Exec(`ALTER TABLE ` + table + ` DROP COLUMN ` + column)
	return err
}

func columnExists(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid        int
			name       string
			colType    string
			notNull    int
			defaultVal any
			pk         int
		)
		if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultVal, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}

// Stats returns cached durable table sizes. It does not query SQLite, so it is
// cheap enough for API calls and Prometheus scrapes.
func (d *DB) Stats() SQLiteStats {
	return SQLiteStats{
		Nodes:               d.stats.nodes.Load(),
		ImportedNodes:       d.stats.importedNodes.Load(),
		Adverts:             d.stats.adverts.Load(),
		ImportedNodeHistory: d.stats.importedNodeHistory.Load(),
	}
}

func (d *DB) setLoadedNodeCount(n int) {
	d.stats.nodes.Store(int64(n))
}

func (d *DB) setImportedNodeCount(n int) {
	d.stats.importedNodes.Store(int64(n))
}

func (d *DB) refreshImportedNodeCount() error {
	var n int64
	if err := d.db.QueryRow(`SELECT COUNT(*) FROM imported_nodes`).Scan(&n); err != nil {
		return err
	}
	d.stats.importedNodes.Store(n)
	return nil
}

func (d *DB) refreshImportedNodeHistoryCount() error {
	var n int64
	if err := d.db.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM imported_node_history`).Scan(&n); err != nil {
		return err
	}
	d.stats.importedNodeHistory.Store(n)
	return nil
}

// Load reads every persisted scope back into memory.
func (d *DB) Load() (map[string]CounterState, error) {
	rows, err := d.db.Query(`SELECT scope, observations, unique_packets, last_packet_at, payload_types, observers, nodes FROM counters`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]CounterState)
	for rows.Next() {
		var (
			scope          string
			pt, obs, nodes string
			st             CounterState
		)
		if err := rows.Scan(&scope, &st.Observations, &st.UniquePackets, &st.LastPacketAt, &pt, &obs, &nodes); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(pt), &st.PayloadTypes)
		_ = json.Unmarshal([]byte(obs), &st.Observers)
		_ = json.Unmarshal([]byte(nodes), &st.Nodes)
		out[scope] = st
	}
	return out, rows.Err()
}

// Save upserts every scope in one transaction.
func (d *DB) Save(states map[string]CounterState, now int64) error {
	d.write.Lock()
	defer d.write.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO counters (scope, observations, unique_packets, last_packet_at, payload_types, observers, nodes, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(scope) DO UPDATE SET
			observations   = excluded.observations,
			unique_packets = excluded.unique_packets,
			last_packet_at = excluded.last_packet_at,
			payload_types  = excluded.payload_types,
			observers      = excluded.observers,
			nodes          = excluded.nodes,
			updated_at     = excluded.updated_at`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for scope, st := range states {
		pt, _ := json.Marshal(st.PayloadTypes)
		obs, _ := json.Marshal(st.Observers)
		nodes, _ := json.Marshal(st.Nodes)
		if _, err := stmt.Exec(scope, st.Observations, st.UniquePackets, st.LastPacketAt, string(pt), string(obs), string(nodes), now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// linkGeoBlob is the on-disk JSON shape for a link's position state: the current
// segment (endpoint coordinates while stationary) plus the frozen history.
type linkGeoBlob struct {
	HasPos         bool             `json:"hasPos"`
	LatA           float64          `json:"latA"`
	LonA           float64          `json:"lonA"`
	LatB           float64          `json:"latB"`
	LonB           float64          `json:"lonB"`
	SegFirstSeen   int64            `json:"segFirstSeen"`
	SegPacketCount uint64           `json:"segPacketCount"`
	Segments       []linkPosSegment `json:"segments,omitempty"`
}

// marshalLinkGeo serializes a link's position state, or "" when there is nothing
// to store (no known positions and no history).
func marshalLinkGeo(rec LinkRecord) string {
	if !rec.HasPos && len(rec.Segments) == 0 {
		return ""
	}
	b, _ := json.Marshal(linkGeoBlob{
		HasPos: rec.HasPos, LatA: rec.LatA, LonA: rec.LonA, LatB: rec.LatB, LonB: rec.LonB,
		SegFirstSeen: rec.SegFirstSeen, SegPacketCount: rec.SegPacketCount, Segments: rec.Segments,
	})
	return string(b)
}

// applyLinkGeo restores a link's position state from its stored JSON blob.
func applyLinkGeo(rec *LinkRecord, s string) {
	if s == "" {
		return
	}
	var g linkGeoBlob
	if json.Unmarshal([]byte(s), &g) != nil {
		return
	}
	rec.HasPos = g.HasPos
	rec.LatA, rec.LonA, rec.LatB, rec.LonB = g.LatA, g.LonA, g.LatB, g.LonB
	rec.SegFirstSeen, rec.SegPacketCount = g.SegFirstSeen, g.SegPacketCount
	rec.Segments = g.Segments
}

// marshalLinkSources serializes a link's per-route-type counts, normalizing the
// empty case to "{}" so the NOT NULL column never stores "null".
func marshalLinkSources(m map[string]uint64) string {
	if len(m) == 0 {
		return "{}"
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func marshalLinkSNRs(vals []float64) string {
	if len(vals) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(vals)
	return string(b)
}

// marshalFlags serializes a node's flag list as a JSON array, normalizing the
// empty/nil case to "[]" so the NOT NULL column never stores "null".
func marshalFlags(flags []string) string {
	if len(flags) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(flags)
	return string(b)
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// LoadNodes reads the persisted node overview rows back into memory. The network
// set is stored as a JSON column; the rolling latest-adverts list is reloaded
// separately from the adverts history (see LoadRecentAdverts).
func (d *DB) LoadNodes() ([]NodeRecord, error) {
	rows, err := d.db.Query(`SELECT pubkey, name, node_type, has_gps, lat, lon, first_advert_at, last_advert_at, advert_count, networks, observer_id, observer_name, flags FROM nodes`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []NodeRecord
	for rows.Next() {
		var (
			n        NodeRecord
			hasGPS   int
			networks string
			flags    string
		)
		if err := rows.Scan(&n.PubKey, &n.Name, &n.NodeType, &hasGPS, &n.Lat, &n.Lon, &n.FirstAdvertAt, &n.LastAdvertAt, &n.AdvertCount, &networks, &n.ObserverID, &n.ObserverName, &flags); err != nil {
			return nil, err
		}
		n.HasGPS = hasGPS != 0
		_ = json.Unmarshal([]byte(networks), &n.Networks)
		_ = json.Unmarshal([]byte(flags), &n.Flags)
		nodes = append(nodes, n)
	}
	return nodes, rows.Err()
}

// SaveNodes upserts the given node overview rows in one transaction, persisting each
// node's network set as JSON.
func (d *DB) SaveNodes(nodes []NodeRecord, now int64) error {
	d.write.Lock()
	defer d.write.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO nodes (pubkey, name, node_type, has_gps, lat, lon, first_advert_at, last_advert_at, advert_count, networks, observer_id, observer_name, flags, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(pubkey) DO UPDATE SET
			name            = excluded.name,
			node_type       = excluded.node_type,
			has_gps         = excluded.has_gps,
			lat             = excluded.lat,
			lon             = excluded.lon,
			first_advert_at = excluded.first_advert_at,
			last_advert_at  = excluded.last_advert_at,
			advert_count    = excluded.advert_count,
			networks        = excluded.networks,
			observer_id     = excluded.observer_id,
			observer_name   = excluded.observer_name,
			flags           = excluded.flags,
			updated_at      = excluded.updated_at`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, n := range nodes {
		networks, _ := json.Marshal(n.Networks)
		flags := marshalFlags(n.Flags)
		if _, err := stmt.Exec(n.PubKey, n.Name, n.NodeType, b2i(n.HasGPS), n.Lat, n.Lon, n.FirstAdvertAt, n.LastAdvertAt, n.AdvertCount, string(networks), n.ObserverID, n.ObserverName, flags, now); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

// AppendAdverts inserts adverts into the append-only history table in one
// transaction. The id column orders them by arrival.
func (d *DB) AppendAdverts(adverts []AdvertObservation) error {
	if len(adverts) == 0 {
		return nil
	}
	d.write.Lock()
	defer d.write.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO adverts (hash, raw_hex, pubkey, name, node_type, has_gps, lat, lon, advert_time, received_at, network_id, analyzer_name, observer_id, observer_name)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, a := range adverts {
		if _, err := stmt.Exec(a.Hash, a.RawHex, a.PubKey, a.Name, a.NodeType, b2i(a.HasGPS), a.Lat, a.Lon, a.AdvertTime, a.At, a.NetworkID, a.AnalyzerName, a.ObserverID, a.ObserverName); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	d.stats.adverts.Add(int64(len(adverts)))
	return nil
}

// SaveLinks batch-upserts the given (dirty) link aggregates and their network
// associations in one transaction. Called from the periodic persistence cycle —
// never per packet. The packet_count is the global deduplicated count and is
// written verbatim; first_seen only moves backwards (MIN) so an out-of-order
// flush can't lose the earliest observation.
func (d *DB) SaveLinks(records []LinkRecord, now int64) error {
	if len(records) == 0 {
		return nil
	}
	d.linkWrite.Lock()
	defer d.linkWrite.Unlock()

	tx, err := d.links.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	linkStmt, err := tx.Prepare(`
		INSERT INTO links (node_a, node_b, packet_count, first_seen, last_seen, activity_score, score_updated_at, last_hash_ab, last_hash_ba, dir_ab, dir_ba, snrs_ab, snrs_ba, geo, sources)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(node_a, node_b) DO UPDATE SET
			packet_count     = excluded.packet_count,
			first_seen       = MIN(links.first_seen, excluded.first_seen),
			last_seen        = MAX(links.last_seen, excluded.last_seen),
			activity_score   = excluded.activity_score,
			score_updated_at = excluded.score_updated_at,
			last_hash_ab     = excluded.last_hash_ab,
			last_hash_ba     = excluded.last_hash_ba,
			dir_ab           = excluded.dir_ab,
			dir_ba           = excluded.dir_ba,
			snrs_ab          = excluded.snrs_ab,
			snrs_ba          = excluded.snrs_ba,
			geo              = excluded.geo,
			sources          = excluded.sources`)
	if err != nil {
		return err
	}
	defer linkStmt.Close()

	netStmt, err := tx.Prepare(`
		INSERT INTO link_networks (node_a, node_b, network_id, first_seen, last_seen)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(node_a, node_b, network_id) DO UPDATE SET
			first_seen = MIN(link_networks.first_seen, excluded.first_seen),
			last_seen  = MAX(link_networks.last_seen, excluded.last_seen)`)
	if err != nil {
		return err
	}
	defer netStmt.Close()

	for _, rec := range records {
		if _, err := linkStmt.Exec(rec.NodeA, rec.NodeB, rec.PacketCount, rec.FirstSeen, rec.LastSeen, rec.Score, rec.ScoreUpdatedAt,
			rec.LastHashAB, rec.LastHashBA, rec.DirAB, rec.DirBA, marshalLinkSNRs(rec.SNRsAB), marshalLinkSNRs(rec.SNRsBA), marshalLinkGeo(rec), marshalLinkSources(rec.SourceCounts)); err != nil {
			return err
		}
		for _, n := range rec.Networks {
			if _, err := netStmt.Exec(rec.NodeA, rec.NodeB, n.NetworkID, n.FirstSeen, n.LastSeen); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// LoadLinks reads every persisted link aggregate plus its network associations
// back into memory at startup.
func (d *DB) LoadLinks() ([]LinkRecord, error) {
	rows, err := d.links.Query(`SELECT node_a, node_b, packet_count, first_seen, last_seen, activity_score, score_updated_at, last_hash_ab, last_hash_ba, dir_ab, dir_ba, snrs_ab, snrs_ba, geo, sources FROM links`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byPair := make(map[[2]string]*LinkRecord)
	var out []LinkRecord
	for rows.Next() {
		var (
			rec     LinkRecord
			geo     string
			sources string
			snrsAB  string
			snrsBA  string
		)
		if err := rows.Scan(&rec.NodeA, &rec.NodeB, &rec.PacketCount, &rec.FirstSeen, &rec.LastSeen, &rec.Score, &rec.ScoreUpdatedAt,
			&rec.LastHashAB, &rec.LastHashBA, &rec.DirAB, &rec.DirBA, &snrsAB, &snrsBA, &geo, &sources); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(snrsAB), &rec.SNRsAB)
		_ = json.Unmarshal([]byte(snrsBA), &rec.SNRsBA)
		applyLinkGeo(&rec, geo)
		if sources != "" {
			_ = json.Unmarshal([]byte(sources), &rec.SourceCounts)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		byPair[[2]string{out[i].NodeA, out[i].NodeB}] = &out[i]
	}

	nrows, err := d.links.Query(`SELECT node_a, node_b, network_id, first_seen, last_seen FROM link_networks`)
	if err != nil {
		return nil, err
	}
	defer nrows.Close()
	for nrows.Next() {
		var (
			a, b, net string
			ln        linkNetwork
		)
		if err := nrows.Scan(&a, &b, &net, &ln.FirstSeen, &ln.LastSeen); err != nil {
			return nil, err
		}
		ln.NetworkID = net
		if rec := byPair[[2]string{a, b}]; rec != nil {
			rec.Networks = append(rec.Networks, ln)
		}
	}
	return out, nrows.Err()
}

// LoadObservers reads every persisted observer activity row back into memory.
func (d *DB) LoadObservers() ([]ObserverRecord, error) {
	rows, err := d.db.Query(`SELECT observer_id, name, first_seen, last_seen, observations, networks FROM observers`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var observers []ObserverRecord
	for rows.Next() {
		var (
			o        ObserverRecord
			networks string
		)
		if err := rows.Scan(&o.ObserverID, &o.Name, &o.FirstSeen, &o.LastSeen, &o.Observations, &networks); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(networks), &o.Networks)
		observers = append(observers, o)
	}
	return observers, rows.Err()
}

// SaveObservers upserts the given observer rows in one transaction, persisting each
// observer's network set as JSON.
func (d *DB) SaveObservers(observers []ObserverRecord, now int64) error {
	d.write.Lock()
	defer d.write.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO observers (observer_id, name, first_seen, last_seen, observations, networks, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(observer_id) DO UPDATE SET
			name         = excluded.name,
			first_seen   = excluded.first_seen,
			last_seen    = excluded.last_seen,
			observations = excluded.observations,
			networks     = excluded.networks,
			updated_at   = excluded.updated_at`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, o := range observers {
		networks, _ := json.Marshal(o.Networks)
		if _, err := stmt.Exec(o.ObserverID, o.Name, o.FirstSeen, o.LastSeen, o.Observations, string(networks), now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// LoadRecentAdverts returns up to perNode most recent adverts for each requested
// pubkey (newest first), keyed by pubkey, used to repopulate each node's
// in-memory rolling list on startup. It performs bounded index lookups against
// idx_adverts_pubkey(pubkey, id) instead of ranking the whole adverts table.
func (d *DB) LoadRecentAdverts(pubkeys []string, perNode int) (map[string][]AdvertObservation, error) {
	if perNode <= 0 || len(pubkeys) == 0 {
		return map[string][]AdvertObservation{}, nil
	}
	stmt, err := d.db.Prepare(`
		SELECT hash, raw_hex, pubkey, name, node_type, has_gps, lat, lon, advert_time, received_at, network_id, analyzer_name, observer_id, observer_name
		FROM adverts
		WHERE pubkey = ?
		ORDER BY id DESC
		LIMIT ?`)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	out := make(map[string][]AdvertObservation)
	for _, pubkey := range pubkeys {
		if pubkey == "" {
			continue
		}
		rows, err := stmt.Query(pubkey, perNode)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var (
				a      AdvertObservation
				hasGPS int
			)
			if err := rows.Scan(&a.Hash, &a.RawHex, &a.PubKey, &a.Name, &a.NodeType, &hasGPS, &a.Lat, &a.Lon, &a.AdvertTime, &a.At, &a.NetworkID, &a.AnalyzerName, &a.ObserverID, &a.ObserverName); err != nil {
				_ = rows.Close()
				return nil, err
			}
			a.HasGPS = hasGPS != 0
			out[a.PubKey] = append(out[a.PubKey], a)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// AdvertsForNode returns one node's adverts from the append-only history table,
// newest first, for the directory's per-node advert history. It pages with a
// keyset cursor: when before > 0 only adverts with a smaller row id are returned,
// so the caller fetches older pages by passing back the nextBefore it received.
// nextBefore is the smallest id in the returned batch (0 when none), suitable as
// the cursor for the next page; it is only meaningful when len(out) == limit.
func (d *DB) AdvertsForNode(pubkey string, limit int, before int64) (out []AdvertObservation, nextBefore int64, err error) {
	q := `SELECT id, hash, raw_hex, pubkey, name, node_type, has_gps, lat, lon, advert_time, received_at, network_id, analyzer_name, observer_id, observer_name
		FROM adverts WHERE pubkey = ?`
	args := []any{pubkey}
	if before > 0 {
		q += ` AND id < ?`
		args = append(args, before)
	}
	q += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := d.db.Query(q, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id     int64
			a      AdvertObservation
			hasGPS int
		)
		if err := rows.Scan(&id, &a.Hash, &a.RawHex, &a.PubKey, &a.Name, &a.NodeType, &hasGPS, &a.Lat, &a.Lon, &a.AdvertTime, &a.At, &a.NetworkID, &a.AnalyzerName, &a.ObserverID, &a.ObserverName); err != nil {
			return nil, 0, err
		}
		a.HasGPS = hasGPS != 0
		out = append(out, a)
		nextBefore = id // rows are id-descending, so the last scanned id is the smallest
	}
	return out, nextBefore, rows.Err()
}

// NetworkAdvertStat is one node's advert activity on a single network.
type NetworkAdvertStat struct {
	NetworkID string `json:"networkId"`
	Adverts   int64  `json:"adverts"`
	FirstAt   int64  `json:"firstAt"`
	LastAt    int64  `json:"lastAt"`
}

// DailyAdvertStat is one UTC day of advert activity for a node.
type DailyAdvertStat struct {
	Day     int64 `json:"day"`
	Adverts int64 `json:"adverts"`
}

// NetworkAdvertStatsForNode aggregates one node's adverts per network from the
// history table: how many adverts, and the first/last time one arrived on each
// network. Ordered most-recently-active first.
func (d *DB) NetworkAdvertStatsForNode(pubkey string) ([]NetworkAdvertStat, error) {
	rows, err := d.db.Query(`
		SELECT network_id, COUNT(*), MIN(received_at), MAX(received_at)
		FROM adverts WHERE pubkey = ?
		GROUP BY network_id
		ORDER BY MAX(received_at) DESC`, pubkey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []NetworkAdvertStat
	for rows.Next() {
		var s NetworkAdvertStat
		if err := rows.Scan(&s.NetworkID, &s.Adverts, &s.FirstAt, &s.LastAt); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// DailyAdvertStatsForNode aggregates one node's advert counts per UTC day.
func (d *DB) DailyAdvertStatsForNode(pubkey string, since int64) ([]DailyAdvertStat, error) {
	rows, err := d.db.Query(`
		SELECT (received_at / 86400) * 86400 AS day, COUNT(*)
		FROM adverts
		WHERE pubkey = ? AND received_at >= ?
		GROUP BY day
		ORDER BY day ASC`, pubkey, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DailyAdvertStat
	for rows.Next() {
		var s DailyAdvertStat
		if err := rows.Scan(&s.Day, &s.Adverts); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// SaveImportedNodes mirrors the external directory into the imported_nodes table
// in one transaction, upserting every node by public key. This table is kept
// entirely separate from the live `nodes` registry.
func (d *DB) SaveImportedNodes(nodes []*ImportedNode, now int64) error {
	d.write.Lock()
	defer d.write.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO imported_nodes (public_key, type, adv_name, last_advert, last_advert_at, adv_lat, adv_lon, inserted_date, updated_date, params, link, source, inserted_by, updated_by, synced_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(public_key) DO UPDATE SET
			type           = excluded.type,
			adv_name       = excluded.adv_name,
			last_advert    = excluded.last_advert,
			last_advert_at = excluded.last_advert_at,
			adv_lat        = excluded.adv_lat,
			adv_lon        = excluded.adv_lon,
			inserted_date  = excluded.inserted_date,
			updated_date   = excluded.updated_date,
			params         = excluded.params,
			link           = excluded.link,
			source         = excluded.source,
			inserted_by    = excluded.inserted_by,
			updated_by     = excluded.updated_by,
			synced_at      = excluded.synced_at`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, n := range nodes {
		if n.PublicKey == "" {
			continue
		}
		params := string(n.Params)
		if params == "" {
			params = "{}"
		}
		if _, err := stmt.Exec(n.PublicKey, n.Type, n.AdvName, n.LastAdvert, n.lastAdvertUnix(), n.AdvLat, n.AdvLon, n.InsertedDate, n.UpdatedDate, params, n.Link, n.Source, n.InsertedBy, n.UpdatedBy, now); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return d.refreshImportedNodeCount()
}

// LoadImportedNodes reads the mirrored directory back into memory so the map has
// data immediately on startup, before the first sync completes.
func (d *DB) LoadImportedNodes() ([]*ImportedNode, error) {
	rows, err := d.db.Query(`SELECT public_key, type, adv_name, last_advert, adv_lat, adv_lon, inserted_date, updated_date, params, link, source, inserted_by, updated_by FROM imported_nodes`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []*ImportedNode
	for rows.Next() {
		var (
			n      ImportedNode
			params string
		)
		if err := rows.Scan(&n.PublicKey, &n.Type, &n.AdvName, &n.LastAdvert, &n.AdvLat, &n.AdvLon, &n.InsertedDate, &n.UpdatedDate, &params, &n.Link, &n.Source, &n.InsertedBy, &n.UpdatedBy); err != nil {
			return nil, err
		}
		n.Params = json.RawMessage(params)
		n.cacheDerived()
		nodes = append(nodes, &n)
	}
	return nodes, rows.Err()
}

// SaveImportedNodeHistory appends a history row for every node whose current
// snapshot we have not recorded before, in one transaction. Dedup is by
// (public_key, sig): a snapshot already on file just has its last_captured_at
// (and last_advert) refreshed, so the table grows only when a node is actually
// re-published with changed metadata.
func (d *DB) SaveImportedNodeHistory(nodes []*ImportedNode, now int64) error {
	d.write.Lock()
	defer d.write.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO imported_node_history (public_key, sig, type, adv_name, last_advert, last_advert_at, adv_lat, adv_lon, inserted_date, updated_date, params, link, source, inserted_by, updated_by, first_captured_at, last_captured_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(public_key, sig) DO UPDATE SET
			last_advert      = excluded.last_advert,
			last_advert_at   = excluded.last_advert_at,
			last_captured_at = excluded.last_captured_at`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, n := range nodes {
		if n.PublicKey == "" {
			continue
		}
		params := string(n.Params)
		if params == "" {
			params = "{}"
		}
		if _, err := stmt.Exec(n.PublicKey, n.historySig(), n.Type, n.AdvName, n.LastAdvert, n.lastAdvertUnix(), n.AdvLat, n.AdvLon, n.InsertedDate, n.UpdatedDate, params, n.Link, n.Source, n.InsertedBy, n.UpdatedBy, now, now); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return d.refreshImportedNodeHistoryCount()
}

// ImportedNodeHistory returns every captured publish for one node, newest
// publish first (by when we first captured it).
func (d *DB) ImportedNodeHistory(pubkey string) ([]ImportedSnapshot, error) {
	rows, err := d.db.Query(`
		SELECT type, adv_name, last_advert, last_advert_at, adv_lat, adv_lon, inserted_date, updated_date, params, link, source, inserted_by, updated_by, first_captured_at, last_captured_at
		FROM imported_node_history
		WHERE public_key = ?
		ORDER BY first_captured_at DESC, id DESC`, pubkey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ImportedSnapshot
	for rows.Next() {
		var (
			s      ImportedSnapshot
			params string
		)
		if err := rows.Scan(&s.Type, &s.AdvName, &s.LastAdvert, &s.LastAdvertAt, &s.AdvLat, &s.AdvLon, &s.InsertedDate, &s.UpdatedDate, &params, &s.Link, &s.Source, &s.InsertedBy, &s.UpdatedBy, &s.FirstCapturedAt, &s.LastCapturedAt); err != nil {
			return nil, err
		}
		if params != "" && params != "{}" {
			s.Params = json.RawMessage(params)
		}
		s.TypeName = nodeTypeName(byte(s.Type))
		s.HasGPS = s.AdvLat != 0 || s.AdvLon != 0
		out = append(out, s)
	}
	return out, rows.Err()
}
