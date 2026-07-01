package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
)

// Duration lets TOML config files use Go duration strings such as "15m" or
// "24h" instead of nanosecond integers.
type Duration time.Duration

func (d *Duration) UnmarshalText(text []byte) error {
	v := strings.TrimSpace(string(text))
	if v == "" {
		*d = 0
		return nil
	}
	parsed, err := time.ParseDuration(v)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) Std() time.Duration {
	return time.Duration(d)
}

// AppConfig is the runtime configuration loaded from TOML and optionally
// overridden by flags.
type AppConfig struct {
	Addr                    string   `toml:"addr"`
	DataURL                 string   `toml:"data_url"`
	AllowOrigin             string   `toml:"allow_origin"`
	TangleveilURL           string   `toml:"tangleveil"`
	NetworkIDs              []string `toml:"networks"`
	NetworkUpdateInterval   Duration `toml:"network_update_interval"`
	DedupWindow             Duration `toml:"dedup_window"`
	LinkHalfLife            Duration `toml:"link_halflife"`
	ObserverTTL             Duration `toml:"observer_ttl"`
	DBPath                  string   `toml:"db"`
	LinksDBPath             string   `toml:"links_db"`
	PersistInterval         Duration `toml:"persist_interval"`
	CounterPersistInterval  Duration `toml:"counter_persist_interval"`
	NodePersistInterval     Duration `toml:"node_persist_interval"`
	AdvertPersistInterval   Duration `toml:"advert_persist_interval"`
	LinkPersistInterval     Duration `toml:"link_persist_interval"`
	ObserverPersistInterval Duration `toml:"observer_persist_interval"`
	ImportURL               string   `toml:"import_url"`
	ImportInterval          Duration `toml:"import_interval"`
}

func DefaultAppConfig() AppConfig {
	return AppConfig{
		Addr:                    ":8080",
		DataURL:                 "https://meshcore.ninja/networks.json",
		AllowOrigin:             "*",
		TangleveilURL:           "wss://tangleveil.meshcore.ninja/ws",
		NetworkUpdateInterval:   Duration(5 * time.Minute),
		DedupWindow:             Duration(15 * time.Minute),
		LinkHalfLife:            Duration(24 * time.Hour),
		ObserverTTL:             Duration(time.Hour),
		DBPath:                  "core.db",
		LinksDBPath:             "links.db",
		PersistInterval:         Duration(20 * time.Second),
		CounterPersistInterval:  Duration(20 * time.Second),
		NodePersistInterval:     Duration(20 * time.Second),
		AdvertPersistInterval:   Duration(20 * time.Second),
		LinkPersistInterval:     Duration(20 * time.Second),
		ObserverPersistInterval: Duration(12 * time.Second),
		ImportURL:               defaultImportURL,
		ImportInterval:          Duration(time.Hour),
	}
}

func LoadAppConfig(path string, required bool) (AppConfig, error) {
	cfg := DefaultAppConfig()
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && !required {
			return cfg, nil
		}
		return cfg, fmt.Errorf("reading config %s: %w", path, err)
	}
	if err := toml.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("parsing config %s: %w", path, err)
	}
	applyPersistIntervalFallbacks(raw, &cfg)
	return cfg, nil
}

func applyPersistIntervalFallbacks(raw []byte, cfg *AppConfig) {
	var keys map[string]any
	if err := toml.Unmarshal(raw, &keys); err != nil {
		return
	}
	if _, ok := keys["counter_persist_interval"]; !ok {
		cfg.CounterPersistInterval = cfg.PersistInterval
	}
	if _, ok := keys["node_persist_interval"]; !ok {
		cfg.NodePersistInterval = cfg.PersistInterval
	}
	if _, ok := keys["advert_persist_interval"]; !ok {
		cfg.AdvertPersistInterval = cfg.PersistInterval
	}
	if _, ok := keys["link_persist_interval"]; !ok {
		cfg.LinkPersistInterval = cfg.PersistInterval
	}
}

func configPathFromArgs(args []string) (path string, explicit bool) {
	path = "config.toml"
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--config" || arg == "-config" {
			if i+1 < len(args) {
				return args[i+1], true
			}
			return path, true
		}
		if strings.HasPrefix(arg, "--config=") {
			return strings.TrimPrefix(arg, "--config="), true
		}
		if strings.HasPrefix(arg, "-config=") {
			return strings.TrimPrefix(arg, "-config="), true
		}
	}
	return path, false
}

func bindConfigFlags(fs *flag.FlagSet, cfg AppConfig, configPath string) *AppConfig {
	out := &AppConfig{NetworkIDs: append([]string(nil), cfg.NetworkIDs...)}
	fs.String("config", configPath, "path to TOML configuration file")
	fs.StringVar(&out.Addr, "addr", cfg.Addr, "HTTP listen address")
	fs.StringVar(&out.DataURL, "data-url", cfg.DataURL, "URL of the published MeshCore Ninja networks.json")
	fs.StringVar(&out.AllowOrigin, "allow-origin", cfg.AllowOrigin, "Access-Control-Allow-Origin value")
	fs.StringVar(&out.TangleveilURL, "tangleveil", cfg.TangleveilURL, "required Tangleveil WebSocket URL (wss://...); direct analyzer connections are not supported")
	fs.DurationVar((*time.Duration)(&out.NetworkUpdateInterval), "network-update-interval", cfg.NetworkUpdateInterval.Std(), "how often to refresh networks from --data-url")
	fs.DurationVar((*time.Duration)(&out.DedupWindow), "dedup-window", cfg.DedupWindow.Std(), "how long a content hash counts as already-seen")
	fs.DurationVar((*time.Duration)(&out.LinkHalfLife), "link-halflife", cfg.LinkHalfLife.Std(), "half-life of a link's recent-activity score")
	fs.DurationVar((*time.Duration)(&out.ObserverTTL), "observer-ttl", cfg.ObserverTTL.Std(), "drop observers/nodes idle longer than this")
	fs.StringVar(&out.DBPath, "db", cfg.DBPath, "SQLite file for persisting counters across restarts (empty = in-memory only)")
	fs.StringVar(&out.LinksDBPath, "links-db", cfg.LinksDBPath, "SQLite file for persisting links when --db is enabled")
	fs.DurationVar((*time.Duration)(&out.PersistInterval), "persist-interval", cfg.PersistInterval.Std(), "default interval for SQLite flushes without a collection-specific override")
	fs.DurationVar((*time.Duration)(&out.CounterPersistInterval), "counter-persist-interval", cfg.CounterPersistInterval.Std(), "how often to flush counters to --db")
	fs.DurationVar((*time.Duration)(&out.NodePersistInterval), "node-persist-interval", cfg.NodePersistInterval.Std(), "how often to flush dirty nodes to --db")
	fs.DurationVar((*time.Duration)(&out.AdvertPersistInterval), "advert-persist-interval", cfg.AdvertPersistInterval.Std(), "how often to flush advert history to --db")
	fs.DurationVar((*time.Duration)(&out.LinkPersistInterval), "link-persist-interval", cfg.LinkPersistInterval.Std(), "how often to flush dirty links to --db")
	fs.DurationVar((*time.Duration)(&out.ObserverPersistInterval), "observer-persist-interval", cfg.ObserverPersistInterval.Std(), "how often to flush observer activity to --db")
	fs.StringVar(&out.ImportURL, "import-url", cfg.ImportURL, "external node directory to mirror (empty = disabled)")
	fs.DurationVar((*time.Duration)(&out.ImportInterval), "import-interval", cfg.ImportInterval.Std(), "how often to sync the external node directory")
	return out
}

func networkIDSet(ids []string) map[string]bool {
	if len(ids) == 0 {
		return nil
	}
	out := map[string]bool{}
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id != "" {
			out[id] = true
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
