package main

import (
	"math"
	"testing"
)

// squareGeoJSON is a FeatureCollection with one network ("box") whose coverage is
// a 2°×2° square centered on the equator/prime meridian (lon/lat in [-1,1]).
const squareGeoJSON = `{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {"networkId": "box", "networkName": "Box"},
      "geometry": {
        "type": "Polygon",
        "coordinates": [[[-1,-1],[1,-1],[1,1],[-1,1],[-1,-1]]]
      }
    },
    {
      "type": "Feature",
      "properties": {"networkId": "multi"},
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [[[[10,10],[11,10],[11,11],[10,11],[10,10]]]]
      }
    }
  ]
}`

func TestParseNetworkAreasGeoJSON(t *testing.T) {
	byNet, err := parseNetworkAreasGeoJSON([]byte(squareGeoJSON))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(byNet) != 2 {
		t.Fatalf("got %d networks, want 2", len(byNet))
	}
	if byNet["box"] == nil || byNet["multi"] == nil {
		t.Fatalf("missing expected networks: %+v", byNet)
	}
	if got := len(byNet["box"].polygons); got != 1 {
		t.Errorf("box polygons = %d, want 1", got)
	}
}

func TestNetworkAreaDistance(t *testing.T) {
	byNet, err := parseNetworkAreasGeoJSON([]byte(squareGeoJSON))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	areas := NewNetworkAreas()
	areas.Replace(byNet)

	// Inside the square → distance 0.
	if d, ok := areas.DistanceKM("box", 0, 0); !ok || d != 0 {
		t.Errorf("inside: got (%.1f, %v), want (0, true)", d, ok)
	}

	// Unknown network → ok=false.
	if _, ok := areas.DistanceKM("nope", 0, 0); ok {
		t.Errorf("unknown network reported ok=true")
	}

	// A point ~1° east of the square's east edge (lon 1) at the equator is about
	// 111 km away; the nearest edge is the meridian lon=1.
	d, ok := areas.DistanceKM("box", 0, 2)
	if !ok {
		t.Fatal("box lookup failed")
	}
	if math.Abs(d-111.0) > 5 {
		t.Errorf("distance to just-east point = %.1f km, want ~111", d)
	}

	// A point far away (~40°N, 0°E) should be well beyond 1000 km.
	if d, _ := areas.DistanceKM("box", 40, 0); d < 1000 {
		t.Errorf("distant point = %.1f km, want >1000", d)
	}
}
