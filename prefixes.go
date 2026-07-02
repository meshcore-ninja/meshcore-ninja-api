package main

import (
	"net/http"
	"sort"
	"strings"
)

// Prefix occupancy endpoint. MeshCore routes on the leading byte(s) of a node's
// public key, so a node picking a fresh key wants a prefix that isn't already
// crowded on its network. This serves the full (uncapped) occupancy histogram
// for a network at a chosen prefix width, so the tools.meshcore.ninja Prefix
// Finder can compute conflicts over every matching node rather than a sample.
//
//	GET /api/prefixes?networks=<id>&bytes=1|2|3&near=<lat,lon>&radius=<km>&types=
//
// `networks` is required (occupancy is only meaningful within one mesh). `bytes`
// is the prefix width in bytes (1 = the MeshCore routing prefix), clamped 1..3.

const maxPrefixBytes = 3

// prefixNode is the lightweight per-node payload carried under each prefix — just
// enough for the UI to name a conflict, without the heavy advert list.
type prefixNode struct {
	PubKey   string `json:"pubkey"`
	Name     string `json:"name"`
	Type     byte   `json:"type"`
	TypeName string `json:"typeName"`
}

type prefixBucket struct {
	Prefix string       `json:"prefix"`
	Count  int          `json:"count"`
	Nodes  []prefixNode `json:"nodes"`
}

func (s *Server) handlePrefixes(w http.ResponseWriter, r *http.Request) {
	p, _, bad := parseSearchParams(s, r)
	if bad != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": bad})
		return
	}
	// Occupancy is scoped to a single mesh; require at least one network so we
	// never dump the whole global registry.
	if len(p.Networks) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "networks parameter is required"})
		return
	}

	bytes := atoiDefault(r.URL.Query().Get("bytes"), 1)
	if bytes < 1 {
		bytes = 1
	}
	if bytes > maxPrefixBytes {
		bytes = maxPrefixBytes
	}
	hexLen := bytes * 2

	// No cap: rank every matching node so the histogram is complete.
	results, _, _ := s.mergedSearch(p, maxInt)

	buckets := make(map[string]*prefixBucket)
	counted := 0
	for _, res := range results {
		pk := strings.ToLower(res.PubKey)
		if len(pk) < hexLen {
			continue
		}
		counted++
		key := pk[:hexLen]
		b := buckets[key]
		if b == nil {
			b = &prefixBucket{Prefix: key}
			buckets[key] = b
		}
		b.Count++
		b.Nodes = append(b.Nodes, prefixNode{
			PubKey:   res.PubKey,
			Name:     res.Name,
			Type:     res.Type,
			TypeName: res.TypeName,
		})
	}

	out := make([]prefixBucket, 0, len(buckets))
	for _, b := range buckets {
		out = append(out, *b)
	}
	// Most-crowded first, then by prefix for a stable order.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Prefix < out[j].Prefix
	})

	// Prefix space is 16^hexLen; for 3 bytes that's ~16.7M and fits an int.
	space := 1
	for i := 0; i < hexLen; i++ {
		space *= 16
	}
	used := len(buckets)

	w.Header().Set("Cache-Control", "public, max-age=15")
	writeJSON(w, http.StatusOK, map[string]any{
		"bytes":      bytes,
		"counted":    counted,
		"space":      space,
		"used":       used,
		"collisions": counted - used,
		"prefixes":   out,
	})
}

// maxInt is a large sentinel limit meaning "return every matching node".
const maxInt = int(^uint(0) >> 1)
