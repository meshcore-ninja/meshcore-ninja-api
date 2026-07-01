package main

import "testing"

// newTestFlagger builds a flagger over the square "box" network area (lon/lat in
// [-1,1]) with the default 1000 km threshold.
func newTestFlagger(t *testing.T) (*Flagger, *NodeRegistry) {
	t.Helper()
	byNet, err := parseNetworkAreasGeoJSON([]byte(squareGeoJSON))
	if err != nil {
		t.Fatalf("parse areas: %v", err)
	}
	areas := NewNetworkAreas()
	areas.Replace(byNet)
	reg := newNodeRegistry(defaultAdvertsPerNode)
	return NewFlagger(reg, areas, 0), reg
}

func TestLocationFlags(t *testing.T) {
	f, _ := newTestFlagger(t)

	cases := []struct {
		name string
		n    NodeRecord
		want []string
	}{
		{"inside box", NodeRecord{HasGPS: true, Lat: 0.5, Lon: 0.5, Networks: []string{"box"}}, nil},
		{"far from box", NodeRecord{HasGPS: true, Lat: 40, Lon: 40, Networks: []string{"box"}}, []string{FlagFarFromNetwork}},
		{"no gps", NodeRecord{HasGPS: false, Lat: 40, Lon: 40, Networks: []string{"box"}}, nil},
		{"no networks", NodeRecord{HasGPS: true, Lat: 40, Lon: 40}, nil},
		{"unknown network only", NodeRecord{HasGPS: true, Lat: 40, Lon: 40, Networks: []string{"ghost"}}, nil},
		// Inside "box" (near) but ~10° (>1000km) from "multi" — impossible membership.
		{"near one, far from another", NodeRecord{HasGPS: true, Lat: 0.5, Lon: 0.5, Networks: []string{"box", "multi"}}, []string{FlagNetworkTooFar}},
		// Inside "multi" but far from "box" — symmetric case.
		{"inside multi, far from box", NodeRecord{HasGPS: true, Lat: 10.5, Lon: 10.5, Networks: []string{"box", "multi"}}, []string{FlagNetworkTooFar}},
		{"far from all known", NodeRecord{HasGPS: true, Lat: 40, Lon: 40, Networks: []string{"box", "multi"}}, []string{FlagFarFromNetwork}},
	}
	for _, c := range cases {
		got := f.locationFlags(&c.n)
		if !sameFlags(got, c.want) {
			t.Errorf("%s: locationFlags = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestScanOnceAppliesFlags(t *testing.T) {
	f, reg := newTestFlagger(t)
	reg.Observe(AdvertObservation{PubKey: "near", HasGPS: true, Lat: 0.5, Lon: 0.5, NetworkID: "box", At: 1})
	reg.Observe(AdvertObservation{PubKey: "far", HasGPS: true, Lat: 40, Lon: 40, NetworkID: "box", At: 1})

	f.scanOnce()

	if v, _ := reg.GetView("near"); len(v.Flags) != 0 {
		t.Errorf("near node flags = %v, want none", v.Flags)
	}
	v, _ := reg.GetView("far")
	if len(v.Flags) != 1 || v.Flags[0] != FlagFarFromNetwork {
		t.Errorf("far node flags = %v, want [%s]", v.Flags, FlagFarFromNetwork)
	}

	flagged := reg.FlaggedNodes()
	if len(flagged) != 1 || flagged[0].PubKey != "far" {
		t.Errorf("FlaggedNodes = %+v, want just 'far'", flagged)
	}

	// A second scan with no changes must report zero changed and keep the flag.
	f.scanOnce()
	if v, _ := reg.GetView("far"); len(v.Flags) != 1 {
		t.Errorf("far node lost its flag on rescan: %v", v.Flags)
	}
}

func TestScanOnceClearsResolvedFlag(t *testing.T) {
	f, reg := newTestFlagger(t)
	reg.Observe(AdvertObservation{PubKey: "n", HasGPS: true, Lat: 40, Lon: 40, NetworkID: "box", At: 1})
	f.scanOnce()
	if v, _ := reg.GetView("n"); len(v.Flags) != 1 {
		t.Fatalf("expected node flagged, got %v", v.Flags)
	}
	// Node moves back inside coverage; the flag should clear on the next scan.
	reg.Observe(AdvertObservation{PubKey: "n", HasGPS: true, Lat: 0.5, Lon: 0.5, NetworkID: "box", At: 2})
	f.scanOnce()
	if v, _ := reg.GetView("n"); len(v.Flags) != 0 {
		t.Errorf("flag not cleared after node returned to coverage: %v", v.Flags)
	}
}
