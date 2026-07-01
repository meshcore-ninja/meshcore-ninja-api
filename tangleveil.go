package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// tvEnvelope is the outer frame Tangleveil emits. The payload field contains
// the CoreScope wsEnvelope JSON ({"type":"packet","data":{...}}) that the
// existing Collector.handle already knows how to process.
type tvEnvelope struct {
	Source  string          `json:"source"`
	Payload json.RawMessage `json:"payload"`
}

// tvSource is one entry from the Tangleveil /sources endpoint.
// mapping is "networkID:analyzerIndex" (e.g. "bay-area-meshcore-2:1").
type tvSource struct {
	ID      string `json:"id"`
	URL     string `json:"url"`
	Mapping string `json:"mapping"`
	State   string `json:"state"`
}

// TangleveilCollector connects to a single Tangleveil WebSocket that
// multiplexes all CoreScope streams and routes each message to the correct
// per-network Collector by the source id field.
type TangleveilCollector struct {
	wsURL   string        // WebSocket URL to connect to
	baseURL string        // HTTP base URL for /sources polling
	store   *Store        // needed to refresh routes on reconnect
	nodes   *NodeRegistry
	obs     *ObserverRegistry
	links   *LinkRegistry
	metrics *Metrics
	routes  map[string]*Collector // Tangleveil source id → handler (rebuilt per connect)
}

// NewTangleveilCollector fetches /sources from Tangleveil to build the initial
// routing table. mapping "networkID:N" points at the Nth AnalyzerState in the
// named network — the same objects the REST API already exposes, so connection
// state is visible without any extra virtual states.
func NewTangleveilCollector(wsURL string, store *Store, nodes *NodeRegistry, observers *ObserverRegistry, links *LinkRegistry, metrics *Metrics) (*TangleveilCollector, error) {
	base, err := wsToHTTP(wsURL)
	if err != nil {
		return nil, fmt.Errorf("bad tangleveil URL %q: %w", wsURL, err)
	}
	tc := &TangleveilCollector{
		wsURL:   wsURL,
		baseURL: base,
		store:   store,
		nodes:   nodes,
		obs:     observers,
		links:   links,
		metrics: metrics,
	}
	routes, err := tc.fetchRoutes()
	if err != nil {
		return nil, err
	}
	tc.routes = routes
	return tc, nil
}

// fetchRoutes calls /sources and maps each source id to a Collector backed by
// the matching AnalyzerState already in the store.
func (t *TangleveilCollector) fetchRoutes() (map[string]*Collector, error) {
	resp, err := http.Get(t.baseURL + "/sources")
	if err != nil {
		return nil, fmt.Errorf("tangleveil /sources: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("tangleveil /sources read: %w", err)
	}
	var sources []tvSource
	if err := json.Unmarshal(body, &sources); err != nil {
		return nil, fmt.Errorf("tangleveil /sources parse: %w", err)
	}

	routes := make(map[string]*Collector, len(sources))
	for _, src := range sources {
		ns, az, err := t.resolveMapping(src.Mapping)
		if err != nil {
			log.Printf("[tangleveil] skipping source %q: %v", src.ID, err)
			continue
		}
		routes[src.ID] = &Collector{
			net:       ns,
			az:        az,
			nodes:     t.nodes,
			observers: t.obs,
			links:     t.links,
			metrics:   t.metrics,
		}
	}
	if len(routes) == 0 {
		return nil, fmt.Errorf("tangleveil: /sources returned no mappings that match our networks")
	}
	return routes, nil
}

// resolveMapping parses "networkID:analyzerIndex" and returns the matching
// NetworkState and AnalyzerState from the store.
func (t *TangleveilCollector) resolveMapping(mapping string) (*NetworkState, *AnalyzerState, error) {
	parts := strings.SplitN(mapping, ":", 2)
	if len(parts) != 2 {
		return nil, nil, fmt.Errorf("unexpected mapping format %q (want networkID:index)", mapping)
	}
	netID := parts[0]
	idx, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, nil, fmt.Errorf("mapping %q: non-integer index: %w", mapping, err)
	}
	ns := t.store.Network(netID)
	if ns == nil {
		return nil, nil, fmt.Errorf("network %q not in store", netID)
	}
	if idx < 0 || idx >= len(ns.Analyzers) {
		return nil, nil, fmt.Errorf("network %q has no analyzer at index %d", netID, idx)
	}
	return ns, ns.Analyzers[idx], nil
}

func (t *TangleveilCollector) Run(ctx context.Context) {
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		if err := t.connectAndRead(ctx); err != nil && ctx.Err() == nil {
			t.markAllDisconnected(err.Error())
			log.Printf("[tangleveil] %v (retry in %s)", err, backoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
		// Refresh the routing table on reconnect — sources may have changed.
		if routes, err := t.fetchRoutes(); err != nil {
			log.Printf("[tangleveil] /sources refresh failed: %v (using previous routes)", err)
		} else {
			t.routes = routes
		}
	}
}

func (t *TangleveilCollector) connectAndRead(ctx context.Context) error {
	u, _ := url.Parse(t.wsURL)
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	header := http.Header{
		"Origin":     {"https://" + u.Host},
		"User-Agent": {browserUA},
	}

	dctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	conn, _, err := dialer.DialContext(dctx, t.wsURL, header)
	cancel()
	if err != nil {
		return fmt.Errorf("dial %s: %w", t.wsURL, err)
	}
	defer conn.Close()

	now := nowUnix()
	for _, col := range t.routes {
		col.az.setConnected(now)
		t.metrics.setAnalyzerConnected(col.net.ID, col.az.Name, true)
		t.metrics.incAnalyzerReconnect(col.net.ID, col.az.Name)
	}
	log.Printf("[tangleveil] connected %s (%d source(s))", t.wsURL, len(t.routes))
	defer t.markAllDisconnected("")

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		t.handle(data)
	}
}

func (t *TangleveilCollector) handle(data []byte) {
	var env tvEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.metrics.recordDecodeError("tv_envelope_json")
		return
	}
	if len(env.Payload) == 0 {
		return
	}
	col, ok := t.routes[env.Source]
	if !ok {
		return // source not in our network set
	}
	col.handle(env.Payload)
}

func (t *TangleveilCollector) markAllDisconnected(errMsg string) {
	for _, col := range t.routes {
		col.az.setDisconnected(errMsg)
		t.metrics.setAnalyzerConnected(col.net.ID, col.az.Name, false)
	}
}

// wsToHTTP converts wss:// → https:// and ws:// → http://, stripping any path
// so callers can append their own (e.g. /sources).
func wsToHTTP(wsURL string) (string, error) {
	u, err := url.Parse(wsURL)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "wss":
		u.Scheme = "https"
	case "ws":
		u.Scheme = "http"
	default:
		return "", fmt.Errorf("expected ws:// or wss://, got %q", u.Scheme)
	}
	u.Path = ""
	u.RawQuery = ""
	return u.String(), nil
}
