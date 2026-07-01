package main

import "testing"

func TestParseNetworksJSON(t *testing.T) {
	raw := []byte(`[
		{
			"id": "net-b",
			"name": "Network B",
			"coverage": {"countries": ["us", "US", "ca"]},
			"radio": {"frequency": "915"},
			"analyzers": [{"name": "b1", "url": "https://b.example"}]
		},
		{
			"id": "net-a",
			"name": "Network A",
			"coverage": {"countries": ["cz"]},
			"radio": {"frequency": "868"},
			"analyzers": [{"name": "a1", "url": "https://a.example"}]
		},
		{
			"id": "ignored",
			"name": "No analyzers"
		}
	]`)

	got, err := parseNetworksJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("networks = %d, want 2", len(got))
	}
	if got[0].ID != "net-a" || got[1].ID != "net-b" {
		t.Fatalf("network order = [%s %s], want [net-a net-b]", got[0].ID, got[1].ID)
	}
	if got[1].Countries[0] != "US" || got[1].Countries[1] != "CA" {
		t.Fatalf("countries = %+v, want [US CA]", got[1].Countries)
	}
	if got[1].Regions[0] != "US915" {
		t.Fatalf("regions = %+v, want [US915]", got[1].Regions)
	}
}
