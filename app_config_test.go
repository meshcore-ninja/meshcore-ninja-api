package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestLoadAppConfigTomlOverridesDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	raw := []byte(`
addr = ":9090"
data_url = "https://example.test/networks.json"
allow_origin = "https://meshcore.ninja"
tangleveil = "wss://example.test/ws"
networks = ["net-a", "net-b"]
network_update_interval = "10m"
dedup_window = "30m"
link_halflife = "12h"
observer_ttl = "2h"
db = "/state/core.db"
links_db = "/state/links.db"
persist_interval = "45s"
counter_persist_interval = "30s"
node_persist_interval = "20s"
advert_persist_interval = "10s"
link_persist_interval = "1m"
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

	if cfg.Addr != ":9090" || cfg.DataURL != "https://example.test/networks.json" || cfg.AllowOrigin != "https://meshcore.ninja" {
		t.Fatalf("basic fields not loaded: %+v", cfg)
	}
	if cfg.TangleveilURL != "wss://example.test/ws" || cfg.DBPath != "/state/core.db" || cfg.LinksDBPath != "/state/links.db" || cfg.ImportURL != "" {
		t.Fatalf("URL/path fields not loaded: %+v", cfg)
	}
	if !reflect.DeepEqual(cfg.NetworkIDs, []string{"net-a", "net-b"}) {
		t.Fatalf("networks = %+v, want [net-a net-b]", cfg.NetworkIDs)
	}
	if cfg.DedupWindow.Std() != 30*time.Minute ||
		cfg.NetworkUpdateInterval.Std() != 10*time.Minute ||
		cfg.LinkHalfLife.Std() != 12*time.Hour ||
		cfg.ObserverTTL.Std() != 2*time.Hour ||
		cfg.PersistInterval.Std() != 45*time.Second ||
		cfg.CounterPersistInterval.Std() != 30*time.Second ||
		cfg.NodePersistInterval.Std() != 20*time.Second ||
		cfg.AdvertPersistInterval.Std() != 10*time.Second ||
		cfg.LinkPersistInterval.Std() != time.Minute ||
		cfg.ObserverPersistInterval.Std() != 15*time.Second ||
		cfg.ImportInterval.Std() != 3*time.Hour {
		t.Fatalf("duration fields not loaded: %+v", cfg)
	}
}

func TestPersistIntervalFallbacks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	raw := []byte(`
persist_interval = "45s"
observer_persist_interval = "15s"
`)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadAppConfig(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CounterPersistInterval.Std() != 45*time.Second ||
		cfg.NodePersistInterval.Std() != 45*time.Second ||
		cfg.AdvertPersistInterval.Std() != 45*time.Second ||
		cfg.LinkPersistInterval.Std() != 45*time.Second {
		t.Fatalf("persist interval fallback not applied: %+v", cfg)
	}
	if cfg.ObserverPersistInterval.Std() != 15*time.Second {
		t.Fatalf("observer interval = %s, want 15s", cfg.ObserverPersistInterval.Std())
	}
}

func TestLoadAppConfigMissingOptionalUsesDefaults(t *testing.T) {
	cfg, err := LoadAppConfig(filepath.Join(t.TempDir(), "missing.toml"), false)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg, DefaultAppConfig()) {
		t.Fatalf("cfg = %+v, want defaults %+v", cfg, DefaultAppConfig())
	}
}

func TestNetworkIDSetNormalizesConfigList(t *testing.T) {
	got := networkIDSet([]string{" net-a ", "", "net-b", "net-a"})
	want := map[string]bool{"net-a": true, "net-b": true}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("networkIDSet = %+v, want %+v", got, want)
	}
	if got := networkIDSet([]string{"", " "}); got != nil {
		t.Fatalf("networkIDSet(empty) = %+v, want nil", got)
	}
}
