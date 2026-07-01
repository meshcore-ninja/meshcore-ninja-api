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
	DataDir                 string   `toml:"data"`
	AllowOrigin             string   `toml:"allow_origin"`
	TangleveilURL           string   `toml:"tangleveil"`
	DedupWindow             Duration `toml:"dedup_window"`
	LinkHalfLife            Duration `toml:"link_halflife"`
	ObserverTTL             Duration `toml:"observer_ttl"`
	DBPath                  string   `toml:"db"`
	PersistInterval         Duration `toml:"persist_interval"`
	ObserverPersistInterval Duration `toml:"observer_persist_interval"`
	ImportURL               string   `toml:"import_url"`
	ImportInterval          Duration `toml:"import_interval"`
}

func DefaultAppConfig() AppConfig {
	return AppConfig{
		Addr:                    ":8080",
		DataDir:                 "../data",
		AllowOrigin:             "*",
		DedupWindow:             Duration(15 * time.Minute),
		LinkHalfLife:            Duration(24 * time.Hour),
		ObserverTTL:             Duration(time.Hour),
		DBPath:                  "meshcore.db",
		PersistInterval:         Duration(20 * time.Second),
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
	return cfg, nil
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
	out := &AppConfig{}
	fs.String("config", configPath, "path to TOML configuration file")
	fs.StringVar(&out.Addr, "addr", cfg.Addr, "HTTP listen address")
	fs.StringVar(&out.DataDir, "data", cfg.DataDir, "path to the MeshCore Ninja catalog data/ directory")
	fs.StringVar(&out.AllowOrigin, "allow-origin", cfg.AllowOrigin, "Access-Control-Allow-Origin value")
	fs.StringVar(&out.TangleveilURL, "tangleveil", cfg.TangleveilURL, "Tangleveil WebSocket URL (wss://...); when set, all CoreScope streams are consumed through Tangleveil instead of connecting to analyzers directly")
	fs.DurationVar((*time.Duration)(&out.DedupWindow), "dedup-window", cfg.DedupWindow.Std(), "how long a content hash counts as already-seen")
	fs.DurationVar((*time.Duration)(&out.LinkHalfLife), "link-halflife", cfg.LinkHalfLife.Std(), "half-life of a link's recent-activity score")
	fs.DurationVar((*time.Duration)(&out.ObserverTTL), "observer-ttl", cfg.ObserverTTL.Std(), "drop observers/nodes idle longer than this")
	fs.StringVar(&out.DBPath, "db", cfg.DBPath, "SQLite file for persisting counters across restarts (empty = in-memory only)")
	fs.DurationVar((*time.Duration)(&out.PersistInterval), "persist-interval", cfg.PersistInterval.Std(), "how often to flush counters/nodes to --db")
	fs.DurationVar((*time.Duration)(&out.ObserverPersistInterval), "observer-persist-interval", cfg.ObserverPersistInterval.Std(), "how often to flush observer activity to --db")
	fs.StringVar(&out.ImportURL, "import-url", cfg.ImportURL, "external node directory to mirror (empty = disabled)")
	fs.DurationVar((*time.Duration)(&out.ImportInterval), "import-interval", cfg.ImportInterval.Std(), "how often to sync the external node directory")
	return out
}
