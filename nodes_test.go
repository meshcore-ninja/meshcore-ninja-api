package main

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/meshcore-cz/meshpkt"
)

// makeAdvertHex builds a signed ADVERT packet and returns its wire bytes as hex,
// mimicking CoreScope's raw_hex field.
func makeAdvertHex(t *testing.T, id meshpkt.Identity, name string, lat, lon float64) string {
	t.Helper()
	adv := meshpkt.Advert{
		PublicKey: id.PublicKey[:],
		NodeType:  meshpkt.AdvertNodeRepeater,
		Name:      name,
		HasGPS:    true,
		Lat:       lat,
		Lon:       lon,
	}
	signed, err := meshpkt.SignAdvert(id, adv)
	if err != nil {
		t.Fatalf("SignAdvert: %v", err)
	}
	payload, err := meshpkt.EncodeAdvertPayload(signed)
	if err != nil {
		t.Fatalf("EncodeAdvertPayload: %v", err)
	}
	raw, err := meshpkt.EncodePacket(meshpkt.Packet{
		Type:    meshpkt.PayloadAdvert,
		Payload: payload,
	})
	if err != nil {
		t.Fatalf("EncodePacket: %v", err)
	}
	return hex.EncodeToString(raw)
}

// feedPacket marshals a packet record into the CoreScope WS envelope and runs it
// through the collector's handle path.
func feedPacket(c *Collector, p wsPacket) {
	data, _ := json.Marshal(p)
	env, _ := json.Marshal(wsEnvelope{Type: "packet", Data: data})
	c.handle(env)
}

func TestCollectAdvert(t *testing.T) {
	id, err := meshpkt.GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	pubHex := hex.EncodeToString(id.PublicKey[:])

	reg := newNodeRegistry(defaultAdvertsPerNode)
	ns := &NetworkState{ID: "test-net", Counter: newCounter()}
	az := &AnalyzerState{Name: "az1", URL: "https://example.test", Counter: newCounter()}
	c := &Collector{net: ns, az: az, nodes: reg}

	advType := int(meshpkt.PayloadAdvert)
	feedPacket(c, wsPacket{
		Hash:         "h1",
		RawHex:       makeAdvertHex(t, id, "Repeater One", 50.1, 14.4),
		ObserverID:   "obs1",
		ObserverName: "Observer One",
		PayloadType:  &advType,
	})

	nodes := reg.Snapshot()
	if len(nodes) != 1 {
		t.Fatalf("nodes = %d, want 1", len(nodes))
	}
	n := nodes[0]
	if n.PubKey != pubHex {
		t.Errorf("pubkey = %q, want %q", n.PubKey, pubHex)
	}
	if n.Name != "Repeater One" {
		t.Errorf("name = %q, want %q", n.Name, "Repeater One")
	}
	if n.TypeName != "repeater" {
		t.Errorf("typeName = %q, want repeater", n.TypeName)
	}
	if !n.HasGPS || n.Lat < 50.09 || n.Lat > 50.11 {
		t.Errorf("gps = (%v, %v, %v), want ~50.1/14.4", n.HasGPS, n.Lat, n.Lon)
	}
	if n.AdvertCount != 1 {
		t.Errorf("advertCount = %d, want 1", n.AdvertCount)
	}
	if len(n.Networks) != 1 || n.Networks[0] != "test-net" {
		t.Errorf("networks = %v, want [test-net]", n.Networks)
	}
	if len(n.LatestAdverts) != 1 || n.LatestAdverts[0].NetworkID != "test-net" {
		t.Errorf("latestAdverts = %+v, want one entry on test-net", n.LatestAdverts)
	}

	// A non-advert packet must not touch the registry.
	txt := int(meshpkt.PayloadTxtMsg)
	feedPacket(c, wsPacket{Hash: "h2", RawHex: "deadbeef", PayloadType: &txt})
	if nodes := reg.Snapshot(); len(nodes) != 1 {
		t.Errorf("after non-advert: nodes = %d, want 1", len(nodes))
	}
}

func TestNodeDetailMarksObserverNode(t *testing.T) {
	pub := pk(0xab)
	nodes := newNodeRegistry(defaultAdvertsPerNode)
	nodes.nodes[pub] = &NodeRecord{
		PubKey:       pub,
		Name:         "Observer Node",
		NodeType:     2,
		HasGPS:       true,
		Lat:          50.1,
		Lon:          14.4,
		LastAdvertAt: 100,
		AdvertCount:  1,
		Networks:     []string{"mesh"},
	}
	observers := newObserverRegistry()
	observers.Observe(ObserverActivity{ObserverID: pub, Name: "Observer One", NetworkID: "mesh", At: 200})
	srv := NewServer(NewStore(nil), nodes, observers, newLinkRegistry(defaultLinkHalfLife), newImportRegistry(), nil, nil, nil, nil, nil, "*")

	req := httptest.NewRequest(http.MethodGet, "/api/nodes/"+pub, nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var view NodeView
	if err := json.Unmarshal(rr.Body.Bytes(), &view); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !view.IsObserver || view.Observer == nil {
		t.Fatalf("observer marker missing: %+v", view)
	}
	if view.Observer.ObserverID != pub || view.Observer.Name != "Observer One" || view.Observer.Observations != 1 {
		t.Errorf("observer = %+v, want id/name/count", view.Observer)
	}
}

func TestRegistryUpsertNetworksAndRing(t *testing.T) {
	reg := newNodeRegistry(3)

	// Same node heard on two networks: one row, networks set grows, count advances.
	reg.Observe(AdvertObservation{PubKey: "aa", Name: "first", At: 100, NetworkID: "net-a"})
	reg.Observe(AdvertObservation{PubKey: "aa", Name: "second", At: 200, NetworkID: "net-b"})
	reg.Observe(AdvertObservation{PubKey: "aa", Name: "third", At: 300, NetworkID: "net-a"}) // dup network
	nodes := reg.Snapshot()
	if len(nodes) != 1 {
		t.Fatalf("nodes = %d, want 1 (upsert)", len(nodes))
	}
	n := nodes[0]
	if n.Name != "third" || n.AdvertCount != 3 {
		t.Errorf("got name=%q count=%d, want third/3", n.Name, n.AdvertCount)
	}
	if n.FirstAdvertAt != 100 || n.LastAdvertAt != 300 {
		t.Errorf("times = %d/%d, want 100/300", n.FirstAdvertAt, n.LastAdvertAt)
	}
	if len(n.Networks) != 2 || n.Networks[0] != "net-a" || n.Networks[1] != "net-b" {
		t.Errorf("networks = %v, want [net-a net-b] (deduped, first-seen order)", n.Networks)
	}

	// The node's rolling advert list keeps only the newest 3, newest first.
	if len(n.LatestAdverts) != 3 {
		t.Fatalf("latestAdverts = %d, want 3 (capped)", len(n.LatestAdverts))
	}
	wantAt := []int64{300, 200, 100}
	for i, w := range wantAt {
		if n.LatestAdverts[i].At != w {
			t.Errorf("latestAdverts[%d].at = %d, want %d", i, n.LatestAdverts[i].At, w)
		}
	}

	// One more advert evicts the oldest.
	reg.Observe(AdvertObservation{PubKey: "aa", At: 400, NetworkID: "net-a"})
	n = reg.Snapshot()[0]
	if len(n.LatestAdverts) != 3 || n.LatestAdverts[0].At != 400 || n.LatestAdverts[2].At != 200 {
		t.Errorf("after evict: %+v", n.LatestAdverts)
	}
}

func TestRegistryRestoreTrimsAdverts(t *testing.T) {
	reg := newNodeRegistry(2)
	reg.Restore(
		[]NodeRecord{{PubKey: "aa", Name: "n", Networks: []string{"net-a", "net-b"}}},
		map[string][]AdvertObservation{"aa": {{At: 3}, {At: 2}, {At: 1}}},
	)
	nodes := reg.Snapshot()
	if len(nodes) != 1 {
		t.Fatalf("nodes = %d, want 1", len(nodes))
	}
	if len(nodes[0].LatestAdverts) != 2 {
		t.Errorf("latestAdverts = %d, want 2 (trimmed to cap)", len(nodes[0].LatestAdverts))
	}
	if len(nodes[0].Networks) != 2 {
		t.Errorf("networks = %v, want 2", nodes[0].Networks)
	}
}

func TestRegistryPendingAdverts(t *testing.T) {
	reg := newNodeRegistry(defaultAdvertsPerNode)
	reg.Observe(AdvertObservation{PubKey: "aa", At: 1})
	reg.Observe(AdvertObservation{PubKey: "bb", At: 2})

	pending := reg.PendingAdverts()
	if len(pending) != 2 {
		t.Fatalf("pending = %d, want 2", len(pending))
	}

	// A new advert arrives before we clear the two we "persisted".
	reg.Observe(AdvertObservation{PubKey: "cc", At: 3})
	reg.ClearPending(len(pending))

	remaining := reg.PendingAdverts()
	if len(remaining) != 1 || remaining[0].PubKey != "cc" {
		t.Errorf("after clear: %+v, want only cc (the unpersisted one)", remaining)
	}
}

func TestRegistryTakeDirtyAndRequeue(t *testing.T) {
	reg := newNodeRegistry(defaultAdvertsPerNode)
	reg.Restore(
		[]NodeRecord{{PubKey: "restored", Name: "clean", Networks: []string{"net-a"}}},
		nil,
	)
	if dirty := reg.TakeDirty(); len(dirty) != 0 {
		t.Fatalf("restored dirty = %d, want 0", len(dirty))
	}

	reg.Observe(AdvertObservation{PubKey: "aa", Name: "first", At: 100, NetworkID: "net-a"})
	reg.Observe(AdvertObservation{PubKey: "aa", Name: "second", At: 200, NetworkID: "net-b"})
	reg.Observe(AdvertObservation{PubKey: "bb", Name: "other", At: 300, NetworkID: "net-a"})

	dirty := reg.TakeDirty()
	if len(dirty) != 2 {
		t.Fatalf("dirty = %d, want 2", len(dirty))
	}
	byPubKey := map[string]NodeRecord{}
	for _, n := range dirty {
		byPubKey[n.PubKey] = n
		if n.LatestAdverts != nil {
			t.Errorf("%s LatestAdverts = %+v, want nil for persistence copy", n.PubKey, n.LatestAdverts)
		}
	}
	if byPubKey["aa"].Name != "second" || byPubKey["aa"].AdvertCount != 2 {
		t.Errorf("aa dirty copy = %+v, want latest overview", byPubKey["aa"])
	}
	if len(byPubKey["aa"].Networks) != 2 {
		t.Errorf("aa networks = %v, want 2 networks", byPubKey["aa"].Networks)
	}
	if dirty := reg.TakeDirty(); len(dirty) != 0 {
		t.Fatalf("dirty after take = %d, want 0", len(dirty))
	}

	reg.Requeue(dirty)
	if dirty := reg.TakeDirty(); len(dirty) != 2 {
		t.Fatalf("dirty after requeue = %d, want 2", len(dirty))
	}
	if dirty := reg.TakeDirty(); len(dirty) != 0 {
		t.Fatalf("dirty after retry take = %d, want 0", len(dirty))
	}
}

func TestNodeStatsByNetwork(t *testing.T) {
	reg := newNodeRegistry(defaultAdvertsPerNode)

	// cz: a located repeater, a located companion (chat), a located repeater that
	// gets location-flagged, and a repeater with no GPS. sk: one located sensor.
	reg.Observe(AdvertObservation{PubKey: "a1", NodeType: 2, HasGPS: true, NetworkID: "cz", At: 1})
	reg.Observe(AdvertObservation{PubKey: "a2", NodeType: 1, HasGPS: true, NetworkID: "cz", At: 1})
	reg.Observe(AdvertObservation{PubKey: "a3", NodeType: 2, HasGPS: true, NetworkID: "cz", At: 1})
	reg.Observe(AdvertObservation{PubKey: "a4", NodeType: 2, HasGPS: false, NetworkID: "cz", At: 1})
	reg.Observe(AdvertObservation{PubKey: "b1", NodeType: 4, HasGPS: true, NetworkID: "sk", At: 1})

	// a3 is far from its network — excluded from the on-map tallies but still counted in the total.
	reg.ApplyFlags(map[string][]string{"a3": {FlagFarFromNetwork}})

	stats := reg.NodeStatsByNetwork()
	cz := stats["cz"]
	if cz == nil {
		t.Fatal("missing cz stats")
	}
	if cz.Total != 4 {
		t.Errorf("cz Total = %d, want 4", cz.Total)
	}
	if cz.OnMap != 2 {
		t.Errorf("cz OnMap = %d, want 2 (a3 flagged, a4 no GPS)", cz.OnMap)
	}
	if cz.ByType["repeater"] != 1 || cz.ByType["chat"] != 1 {
		t.Errorf("cz ByType = %v, want repeater:1 chat:1", cz.ByType)
	}
	if sk := stats["sk"]; sk == nil || sk.Total != 1 || sk.OnMap != 1 || sk.ByType["sensor"] != 1 {
		t.Errorf("sk stats = %+v, want total/onmap 1 sensor 1", sk)
	}
}
