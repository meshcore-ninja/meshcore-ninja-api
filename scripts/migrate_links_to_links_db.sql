-- Move persisted link graph tables from the core DB into a separate links DB.
--
-- Usage:
--   cd /path/to/state
--   cp core.db core.db.bak
--   sqlite3 core.db ".read /path/to/meshcore-ninja-api/scripts/migrate_links_to_links_db.sql"
--
-- This script copies data into links.db in the current working directory. If you
-- want a different target path, edit the ATTACH line below before running it.

.bail on

ATTACH DATABASE 'links.db' AS linksdb;

PRAGMA linksdb.busy_timeout = 5000;
PRAGMA linksdb.journal_mode = WAL;

BEGIN;

CREATE TABLE IF NOT EXISTS linksdb.links (
	node_a           TEXT    NOT NULL,
	node_b           TEXT    NOT NULL,
	packet_count     INTEGER NOT NULL DEFAULT 0,
	first_seen       INTEGER NOT NULL DEFAULT 0,
	last_seen        INTEGER NOT NULL DEFAULT 0,
	activity_score   REAL    NOT NULL DEFAULT 0,
	score_updated_at INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (node_a, node_b)
);

CREATE INDEX IF NOT EXISTS linksdb.idx_links_node_b ON links(node_b);

CREATE TABLE IF NOT EXISTS linksdb.link_networks (
	node_a     TEXT    NOT NULL,
	node_b     TEXT    NOT NULL,
	network_id TEXT    NOT NULL,
	first_seen INTEGER NOT NULL DEFAULT 0,
	last_seen  INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (node_a, node_b, network_id)
);

INSERT INTO linksdb.links (
	node_a, node_b, packet_count, first_seen, last_seen, activity_score, score_updated_at
)
SELECT
	node_a, node_b, packet_count, first_seen, last_seen, activity_score, score_updated_at
FROM main.links
WHERE true
ON CONFLICT(node_a, node_b) DO UPDATE SET
	packet_count     = excluded.packet_count,
	first_seen       = excluded.first_seen,
	last_seen        = excluded.last_seen,
	activity_score   = excluded.activity_score,
	score_updated_at = excluded.score_updated_at;

INSERT INTO linksdb.link_networks (
	node_a, node_b, network_id, first_seen, last_seen
)
SELECT
	node_a, node_b, network_id, first_seen, last_seen
FROM main.link_networks
WHERE true
ON CONFLICT(node_a, node_b, network_id) DO UPDATE SET
	first_seen = excluded.first_seen,
	last_seen  = excluded.last_seen;

COMMIT;

SELECT 'core.links', COUNT(*) FROM main.links;
SELECT 'linksdb.links', COUNT(*) FROM linksdb.links;
SELECT 'core.link_networks', COUNT(*) FROM main.link_networks;
SELECT 'linksdb.link_networks', COUNT(*) FROM linksdb.link_networks;

-- After verifying the copied row counts and taking a backup, you may remove the
-- old link tables from the core DB manually:
--
-- DROP TABLE main.link_networks;
-- DROP INDEX IF EXISTS main.idx_links_node_b;
-- DROP TABLE main.links;
-- VACUUM;

DETACH DATABASE linksdb;
