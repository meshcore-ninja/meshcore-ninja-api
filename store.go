package main

import (
	"fmt"
	"sync"
)

// AnalyzerState holds the live connection status and per-analyzer metrics for
// one CoreScope analyzer.
type AnalyzerState struct {
	Name    string
	URL     string
	Counter *Counter

	mu             sync.Mutex
	connected      bool
	connectedSince int64
	lastError      string
}

func (a *AnalyzerState) setConnected(now int64) {
	a.mu.Lock()
	a.connected = true
	a.connectedSince = now
	a.lastError = ""
	a.mu.Unlock()
}

func (a *AnalyzerState) setDisconnected(err string) {
	a.mu.Lock()
	a.connected = false
	a.connectedSince = 0
	if err != "" {
		a.lastError = err
	}
	a.mu.Unlock()
}

func (a *AnalyzerState) status() (connected bool, since int64, lastErr string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.connected, a.connectedSince, a.lastError
}

// NetworkState aggregates a network's own (cross-analyzer deduplicated) counter
// plus the state of each analyzer it runs.
type NetworkState struct {
	ID        string
	Name      string
	Countries []string
	Regions   []string
	Counter   *Counter // deduplicated across all analyzers in this network
	Analyzers []*AnalyzerState
}

// Store is the updateable registry of networks and analyzers. Network refreshes
// replace the snapshot while preserving counters for unchanged ids.
type Store struct {
	mu       sync.RWMutex
	Networks []*NetworkState
	byID     map[string]*NetworkState
}

func NewStore(configs []NetworkConfig) *Store {
	s := &Store{}
	s.Update(configs)
	return s
}

func (s *Store) Update(configs []NetworkConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldByID := s.byID
	networks := make([]*NetworkState, 0, len(configs))
	byID := make(map[string]*NetworkState, len(configs))
	for _, nc := range configs {
		var old *NetworkState
		if oldByID != nil {
			old = oldByID[nc.ID]
		}
		counter := newCounter()
		oldAnalyzers := map[string]*AnalyzerState{}
		if old != nil {
			counter = old.Counter
			for _, a := range old.Analyzers {
				oldAnalyzers[a.Name] = a
			}
		}
		ns := &NetworkState{
			ID:        nc.ID,
			Name:      nc.Name,
			Countries: append([]string(nil), nc.Countries...),
			Regions:   append([]string(nil), nc.Regions...),
			Counter:   counter,
		}
		for _, ac := range nc.Analyzers {
			counter := newCounter()
			var connected bool
			var connectedSince int64
			var lastError string
			if oldAZ := oldAnalyzers[ac.Name]; oldAZ != nil {
				counter = oldAZ.Counter
				connected, connectedSince, lastError = oldAZ.status()
			}
			az := &AnalyzerState{
				Name:           ac.Name,
				URL:            ac.URL,
				Counter:        counter,
				connected:      connected,
				connectedSince: connectedSince,
				lastError:      lastError,
			}
			ns.Analyzers = append(ns.Analyzers, az)
		}
		networks = append(networks, ns)
		byID[nc.ID] = ns
	}
	s.Networks = networks
	s.byID = byID
}

func (s *Store) NetworksSnapshot() []*NetworkState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]*NetworkState(nil), s.Networks...)
}

func (s *Store) NetworkCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.Networks)
}

func (s *Store) AnalyzerCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, ns := range s.Networks {
		n += len(ns.Analyzers)
	}
	return n
}

func (s *Store) InitMetrics(metrics *Metrics) {
	for _, ns := range s.NetworksSnapshot() {
		for _, az := range ns.Analyzers {
			metrics.initAnalyzer(ns.ID, az.Name)
		}
	}
}

func (s *Store) Network(id string) *NetworkState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.byID[id]
}

func (s *Store) HasNetwork(id string) bool {
	return s.Network(id) != nil
}

func (s *Store) ValidateNetworkFilter(filter map[string]bool) error {
	for id := range filter {
		if !s.HasNetwork(id) {
			return fmt.Errorf("networks contains unknown network id %q", id)
		}
	}
	return nil
}

// Observe routes a packet event into both the analyzer-scoped counter and the
// network-scoped (cross-analyzer deduplicated) counter.
func (n *NetworkState) Observe(a *AnalyzerState, ev Event) {
	a.Counter.Observe(ev)
	n.Counter.Observe(ev)
}

// scopeKey identifies a counter in the persistence store. Network-wide counters
// are keyed by network id; analyzer counters by "<id>\x1f<analyzer name>".
func netScopeKey(id string) string          { return "net\x1f" + id }
func azScopeKey(id, analyzer string) string { return "az\x1f" + id + "\x1f" + analyzer }

// Export captures every counter's durable state, keyed by scope, for persistence.
func (s *Store) Export() map[string]CounterState {
	networks := s.NetworksSnapshot()
	out := make(map[string]CounterState, len(networks)*2)
	for _, ns := range networks {
		out[netScopeKey(ns.ID)] = ns.Counter.Export()
		for _, a := range ns.Analyzers {
			out[azScopeKey(ns.ID, a.Name)] = a.Counter.Export()
		}
	}
	return out
}

// Restore seeds counters from persisted state. Unknown scopes (e.g. an analyzer
// removed from the data files) are simply ignored.
func (s *Store) Restore(states map[string]CounterState) {
	for _, ns := range s.NetworksSnapshot() {
		if st, ok := states[netScopeKey(ns.ID)]; ok {
			ns.Counter.Restore(st)
		}
		for _, a := range ns.Analyzers {
			if st, ok := states[azScopeKey(ns.ID, a.Name)]; ok {
				a.Counter.Restore(st)
			}
		}
	}
}

// sweep prunes stale dedup/observer entries across every counter.
func (s *Store) sweep(now, dedupWindow, observerTTL int64) {
	for _, ns := range s.NetworksSnapshot() {
		ns.Counter.sweep(now, dedupWindow, observerTTL)
		for _, a := range ns.Analyzers {
			a.Counter.sweep(now, dedupWindow, observerTTL)
		}
	}
}
