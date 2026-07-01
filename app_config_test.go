package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadAppConfigTomlOverridesDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	raw := []byte(`
addr = ":9090"
data = "/catalog/data"
allow_origin = "https://meshcore.ninja"
tangleveil = "wss://example.test/ws"
dedup_window = "30m"
link_halflife = "12h"
observer_ttl = "2h"
db = "/state/meshcore.db"
persist_interval = "45s"
observer_persist_interval = "15s"
import_url = ""
import_interval = "3h"
`)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadAppConfig(path, true)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Addr != ":9090" || cfg.DataDir != "/catalog/data" || cfg.AllowOrigin != "https://meshcore.ninja" {
		t.Fatalf("basic fields not loaded: %+v", cfg)
	}
	if cfg.TangleveilURL != "wss://example.test/ws" || cfg.DBPath != "/state/meshcore.db" || cfg.ImportURL != "" {
		t.Fatalf("URL/path fields not loaded: %+v", cfg)
	}
	if cfg.DedupWindow.Std() != 30*time.Minute ||
		cfg.LinkHalfLife.Std() != 12*time.Hour ||
		cfg.ObserverTTL.Std() != 2*time.Hour ||
		cfg.PersistInterval.Std() != 45*time.Second ||
		cfg.ObserverPersistInterval.Std() != 15*time.Second ||
		cfg.ImportInterval.Std() != 3*time.Hour {
		t.Fatalf("duration fields not loaded: %+v", cfg)
	}
}

func TestLoadAppConfigMissingOptionalUsesDefaults(t *testing.T) {
	cfg, err := LoadAppConfig(filepath.Join(t.TempDir(), "missing.toml"), false)
	if err != nil {
		t.Fatal(err)
	}
	if cfg != DefaultAppConfig() {
		t.Fatalf("cfg = %+v, want defaults %+v", cfg, DefaultAppConfig())
	}
}
