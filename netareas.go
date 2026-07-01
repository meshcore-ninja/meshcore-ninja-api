package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync"
	"time"
)

// NetworkAreas holds the published coverage polygons for each network, loaded
// from the network-area GeoJSON (a FeatureCollection whose features carry a
// "networkId" property and a Polygon/MultiPolygon geometry). It answers "how far
// is this point from network X's coverage?" for the geographic flagging task.
// Safe for concurrent use; the polygon set is swapped atomically on refresh.
type NetworkAreas struct {
	mu    sync.RWMutex
	byNet map[string]*networkArea
}

// networkArea is one network's coverage: a set of polygons (an outer ring
// followed by any hole rings) plus a bounding box for a cheap distance shortcut.
type networkArea struct {
	polygons []polygon
	// bounding box of all outer rings, in degrees.
	minLat, minLon, maxLat, maxLon float64
}

// polygon is an outer ring followed by zero or more hole rings. Each ring is a
// list of [lon, lat] vertices (GeoJSON coordinate order).
type polygon struct {
	rings [][][2]float64
}

// geojsonFeatureCollection is the minimal shape we parse from the area GeoJSON.
// Geometry coordinates are left as json.RawMessage and decoded per geometry type
// because Polygon and MultiPolygon nest to different depths.
type geojsonFeatureCollection struct {
	Features []struct {
		Properties struct {
			NetworkID string `json:"networkId"`
		} `json:"properties"`
		Geometry struct {
			Type        string          `json:"type"`
			Coordinates json.RawMessage `json:"coordinates"`
		} `json:"geometry"`
	} `json:"features"`
}

// NewNetworkAreas returns an empty area set (no network has coverage until it is
// loaded). Callers can still query it; every lookup reports "no area".
func NewNetworkAreas() *NetworkAreas {
	return &NetworkAreas{byNet: map[string]*networkArea{}}
}

// Count returns the number of networks that currently have a coverage area.
func (a *NetworkAreas) Count() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.byNet)
}

// LoadNetworkAreas fetches and parses the network-area GeoJSON from url.
func LoadNetworkAreas(url string) (map[string]*networkArea, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("building network-area request: %w", err)
	}
	req.Header.Set("Accept", "application/geo+json, application/json")
	client := http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching network areas from %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("fetching network areas from %s: HTTP %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading network areas from %s: %w", url, err)
	}
	return parseNetworkAreasGeoJSON(body)
}

func parseNetworkAreasGeoJSON(raw []byte) (map[string]*networkArea, error) {
	var fc geojsonFeatureCollection
	if err := json.Unmarshal(raw, &fc); err != nil {
		return nil, fmt.Errorf("parsing network-area GeoJSON: %w", err)
	}
	out := make(map[string]*networkArea, len(fc.Features))
	for _, f := range fc.Features {
		id := f.Properties.NetworkID
		if id == "" || len(f.Geometry.Coordinates) == 0 {
			continue
		}
		polys, err := decodeGeometry(f.Geometry.Type, f.Geometry.Coordinates)
		if err != nil {
			return nil, fmt.Errorf("network %q geometry: %w", id, err)
		}
		if len(polys) == 0 {
			continue
		}
		na := buildNetworkArea(polys)
		// A feature collection may split one network across several features; merge
		// their polygons under the same id.
		if existing := out[id]; existing != nil {
			existing.polygons = append(existing.polygons, na.polygons...)
			existing.minLat = math.Min(existing.minLat, na.minLat)
			existing.minLon = math.Min(existing.minLon, na.minLon)
			existing.maxLat = math.Max(existing.maxLat, na.maxLat)
			existing.maxLon = math.Max(existing.maxLon, na.maxLon)
		} else {
			out[id] = na
		}
	}
	return out, nil
}

// decodeGeometry normalizes Polygon and MultiPolygon coordinates into a flat
// list of polygons (each an outer ring plus hole rings).
func decodeGeometry(geomType string, coords json.RawMessage) ([]polygon, error) {
	switch geomType {
	case "Polygon":
		var rings [][][2]float64
		if err := json.Unmarshal(coords, &rings); err != nil {
			return nil, err
		}
		if len(rings) == 0 {
			return nil, nil
		}
		return []polygon{{rings: rings}}, nil
	case "MultiPolygon":
		var multi [][][][2]float64
		if err := json.Unmarshal(coords, &multi); err != nil {
			return nil, err
		}
		polys := make([]polygon, 0, len(multi))
		for _, rings := range multi {
			if len(rings) > 0 {
				polys = append(polys, polygon{rings: rings})
			}
		}
		return polys, nil
	default:
		// Points, lines, and geometry collections don't describe an area; skip them.
		return nil, nil
	}
}

func buildNetworkArea(polys []polygon) *networkArea {
	na := &networkArea{
		polygons: polys,
		minLat:   math.Inf(1), minLon: math.Inf(1),
		maxLat: math.Inf(-1), maxLon: math.Inf(-1),
	}
	for _, p := range polys {
		if len(p.rings) == 0 {
			continue
		}
		for _, v := range p.rings[0] { // outer ring bounds the polygon
			lon, lat := v[0], v[1]
			na.minLat = math.Min(na.minLat, lat)
			na.maxLat = math.Max(na.maxLat, lat)
			na.minLon = math.Min(na.minLon, lon)
			na.maxLon = math.Max(na.maxLon, lon)
		}
	}
	return na
}

// Replace swaps in a freshly loaded polygon set atomically.
func (a *NetworkAreas) Replace(byNet map[string]*networkArea) {
	a.mu.Lock()
	a.byNet = byNet
	a.mu.Unlock()
}

// DistanceKM returns the great-circle distance in kilometers from (lat, lon) to
// the nearest point of network id's coverage; it is 0 when the point lies inside
// the coverage. ok is false when the network has no known area, so callers can
// distinguish "far" from "unknown".
func (a *NetworkAreas) DistanceKM(id string, lat, lon float64) (km float64, ok bool) {
	a.mu.RLock()
	area := a.byNet[id]
	a.mu.RUnlock()
	if area == nil {
		return 0, false
	}
	return area.distanceKM(lat, lon), true
}

func (na *networkArea) distanceKM(lat, lon float64) float64 {
	for _, p := range na.polygons {
		if p.contains(lon, lat) {
			return 0
		}
	}
	best := math.Inf(1)
	for _, p := range na.polygons {
		for _, ring := range p.rings {
			d := ringDistanceKM(ring, lat, lon)
			if d < best {
				best = d
			}
		}
	}
	return best
}

// contains reports whether (lon, lat) is inside the polygon: inside its outer
// ring and outside every hole ring.
func (p polygon) contains(lon, lat float64) bool {
	if len(p.rings) == 0 || !ringContains(p.rings[0], lon, lat) {
		return false
	}
	for _, hole := range p.rings[1:] {
		if ringContains(hole, lon, lat) {
			return false
		}
	}
	return true
}

// ringContains is the standard even-odd ray-casting test in lon/lat space. For
// the ~1000km granularity we care about, treating degrees as planar here is
// harmless; the accurate great-circle distance is only computed once a point is
// found to be outside.
func ringContains(ring [][2]float64, lon, lat float64) bool {
	inside := false
	n := len(ring)
	for i, j := 0, n-1; i < n; j, i = i, i+1 {
		xi, yi := ring[i][0], ring[i][1]
		xj, yj := ring[j][0], ring[j][1]
		if (yi > lat) != (yj > lat) {
			xCross := (xj-xi)*(lat-yi)/(yj-yi) + xi
			if lon < xCross {
				inside = !inside
			}
		}
	}
	return inside
}

// ringDistanceKM returns the minimum great-circle distance from (lat, lon) to
// any edge of the ring.
func ringDistanceKM(ring [][2]float64, lat, lon float64) float64 {
	best := math.Inf(1)
	n := len(ring)
	for i := 0; i < n; i++ {
		a := ring[i]
		b := ring[(i+1)%n]
		d := segmentDistanceKM(lat, lon, a[1], a[0], b[1], b[0])
		if d < best {
			best = d
		}
	}
	return best
}

// segmentDistanceKM returns the great-circle distance from point P=(plat, plon)
// to the segment A=(alat, alon)–B=(blat, blon). The nearest point on the segment
// is found in a local equirectangular projection centered at P (accurate for the
// short segments of a simplified coverage ring), then the final distance to that
// nearest point is measured with the exact haversine formula.
func segmentDistanceKM(plat, plon, alat, alon, blat, blon float64) float64 {
	cosLat := math.Cos(plat * math.Pi / 180)
	// Project each endpoint into a plane centered at P; x scaled by cos(lat) so a
	// degree of longitude and a degree of latitude are comparable.
	ax := (alon - plon) * cosLat
	ay := alat - plat
	bx := (blon - plon) * cosLat
	by := blat - plat

	dx := bx - ax
	dy := by - ay
	var t float64
	if seg := dx*dx + dy*dy; seg > 0 {
		// Project origin (P) onto AB, clamped to the segment.
		t = -(ax*dx + ay*dy) / seg
		if t < 0 {
			t = 0
		} else if t > 1 {
			t = 1
		}
	}
	nlon := alon + t*(blon-alon)
	nlat := alat + t*(blat-alat)
	return haversineKM(plat, plon, nlat, nlon)
}
