package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// AnalyzerConfig is one CoreScope analyzer instance belonging to a network.
type AnalyzerConfig struct {
	Name string
	URL  string
}

// NetworkConfig is the subset of a published network record we care about: its
// identity, analyzer list, and coarse metadata used by API filters.
type NetworkConfig struct {
	ID        string
	Name      string
	Analyzers []AnalyzerConfig
	Countries []string
	Regions   []string
}

type networkJSON struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Coverage struct {
		Countries []string `json:"countries"`
	} `json:"coverage"`
	Radio struct {
		Frequency any    `json:"frequency"`
		Region    string `json:"region"`
	} `json:"radio"`
	Radios []struct {
		Frequency any    `json:"frequency"`
		Region    string `json:"region"`
	} `json:"radios"`
	Analyzers []struct {
		Name string `json:"name"`
		URL  string `json:"url"`
	} `json:"analyzers"`
}

func LoadNetworks(url string) ([]NetworkConfig, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("building networks request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	client := http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching networks from %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("fetching networks from %s: HTTP %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading networks from %s: %w", url, err)
	}
	return parseNetworksJSON(body)
}

func parseNetworksJSON(raw []byte) ([]NetworkConfig, error) {
	var records []networkJSON
	if err := json.Unmarshal(raw, &records); err != nil {
		return nil, fmt.Errorf("parsing networks JSON: %w", err)
	}

	out := make([]NetworkConfig, 0, len(records))
	for _, rec := range records {
		if rec.ID == "" || len(rec.Analyzers) == 0 {
			continue
		}
		nc := NetworkConfig{ID: rec.ID, Name: rec.Name}
		seenCountries := map[string]bool{}
		for _, cc := range rec.Coverage.Countries {
			cc = strings.ToUpper(strings.TrimSpace(cc))
			if len(cc) == 2 && !seenCountries[cc] {
				seenCountries[cc] = true
				nc.Countries = append(nc.Countries, cc)
			}
		}
		seenRegions := map[string]bool{}
		addRegion := func(v any, explicit string) {
			r := strings.ToUpper(strings.TrimSpace(explicit))
			if r == "" && v != nil {
				r = strings.ToUpper(strings.TrimSpace(fmt.Sprint(v)))
				switch strings.TrimSuffix(r, "MHZ") {
				case "868":
					r = "EU868"
				case "915":
					r = "US915"
				case "433":
					r = "EU433"
				}
			}
			if r != "" && !seenRegions[r] {
				seenRegions[r] = true
				nc.Regions = append(nc.Regions, r)
			}
		}
		addRegion(rec.Radio.Frequency, rec.Radio.Region)
		for _, r := range rec.Radios {
			addRegion(r.Frequency, r.Region)
		}
		for _, a := range rec.Analyzers {
			if a.URL == "" {
				continue
			}
			nc.Analyzers = append(nc.Analyzers, AnalyzerConfig{Name: a.Name, URL: a.URL})
		}
		if len(nc.Analyzers) > 0 {
			out = append(out, nc)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
