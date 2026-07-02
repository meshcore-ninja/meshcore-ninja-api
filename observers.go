package main

import (
	"sort"
	"strings"
	"sync"
)

// ObserverActivity is one report from an observer node, extracted from a packet
// event. Every observed packet (not just adverts) updates the observer's row.
type ObserverActivity struct {
	ObserverID string
	Name       string
	NetworkID  string
	At         int64 // when we received the report (unix seconds)
}

// ObserverRecord is the durable activity row for one observer node, keyed by
// observer id. It tracks first/last activity, a running report count, and the
// set of networks the observer has reported on. Never aged out — unlike the
// per-scope observer gauge in Counter, this is a persistent activity log.
type ObserverRecord struct {
	ObserverID   string
	Name         string
	FirstSeen    int64
	LastSeen     int64
	Observations uint64
	Networks     []string // set of network IDs reported on, first-seen order
}

// ObserverRegistry is the global (cross-network) store of observer activity.
// Safe for concurrent use. Like the node registry it is kept in memory and
// flushed to SQLite periodically.
type ObserverRegistry struct {
	mu        sync.Mutex
	observers map[string]*ObserverRecord
	dirty     map[string]struct{}
}

func newObserverRegistry() *ObserverRegistry {
	return &ObserverRegistry{
		observers: make(map[string]*ObserverRecord),
		dirty:     make(map[string]struct{}),
	}
}

// canonObserverID normalizes an observer id (a node public key) to the lowercase
// hex form used as the canonical key everywhere else. Analyzers report observer
// ids in upper case, so without this the registry would key rows under a form
// that never matches a node's (lower-cased) pubkey — leaving isObserver false.
func canonObserverID(id string) string {
	return strings.ToLower(strings.TrimSpace(id))
}

// Observe records one observer report: upserts the row, advances last-seen and
// the report count, and adds the network to the observer's set.
func (r *ObserverRegistry) Observe(a ObserverActivity) {
	id := canonObserverID(a.ObserverID)
	if id == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	o := r.observers[id]
	if o == nil {
		o = &ObserverRecord{ObserverID: id, FirstSeen: a.At}
		r.observers[id] = o
	}
	if a.Name != "" {
		o.Name = a.Name
	}
	o.LastSeen = a.At
	o.Observations++
	if a.NetworkID != "" && !containsStr(o.Networks, a.NetworkID) {
		o.Networks = append(o.Networks, a.NetworkID)
	}
	r.dirty[id] = struct{}{}
}

// ObserverView is the JSON shape served by the API.
type ObserverView struct {
	ObserverID   string   `json:"observerId"`
	Name         string   `json:"name"`
	FirstSeen    int64    `json:"firstSeen"`
	LastSeen     int64    `json:"lastSeen"`
	Observations uint64   `json:"observations"`
	Networks     []string `json:"networks"`
}

// Snapshot returns every observer, most recently active first.
func (r *ObserverRegistry) Snapshot() []ObserverView {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ObserverView, 0, len(r.observers))
	for _, o := range r.observers {
		out = append(out, ObserverView{
			ObserverID:   o.ObserverID,
			Name:         o.Name,
			FirstSeen:    o.FirstSeen,
			LastSeen:     o.LastSeen,
			Observations: o.Observations,
			Networks:     append([]string(nil), o.Networks...),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LastSeen != out[j].LastSeen {
			return out[i].LastSeen > out[j].LastSeen
		}
		return out[i].ObserverID < out[j].ObserverID
	})
	return out
}

func (r *ObserverRegistry) Lookup(observerID string) (ObserverView, bool) {
	observerID = canonObserverID(observerID)
	if observerID == "" {
		return ObserverView{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	o := r.observers[observerID]
	if o == nil {
		return ObserverView{}, false
	}
	return ObserverView{
		ObserverID:   o.ObserverID,
		Name:         o.Name,
		FirstSeen:    o.FirstSeen,
		LastSeen:     o.LastSeen,
		Observations: o.Observations,
		Networks:     append([]string(nil), o.Networks...),
	}, true
}

// Export captures every observer row for persistence, deep-copying slices so
// callers can serialize them outside the lock.
func (r *ObserverRegistry) Export() []ObserverRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ObserverRecord, 0, len(r.observers))
	for _, o := range r.observers {
		rec := *o
		rec.Networks = append([]string(nil), o.Networks...)
		out = append(out, rec)
	}
	return out
}

// TakeDirty captures observer rows changed since the previous call and clears
// the dirty set atomically. On a failed persist the caller should re-mark them
// with Requeue so the latest in-memory state is retried.
func (r *ObserverRegistry) TakeDirty() []ObserverRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.dirty) == 0 {
		return nil
	}
	out := make([]ObserverRecord, 0, len(r.dirty))
	for observerID := range r.dirty {
		o := r.observers[observerID]
		if o == nil {
			continue
		}
		rec := *o
		rec.Networks = append([]string(nil), o.Networks...)
		out = append(out, rec)
	}
	r.dirty = make(map[string]struct{})
	return out
}

// Requeue re-marks observer rows dirty after a failed persist so they flush on
// the next cycle.
func (r *ObserverRegistry) Requeue(records []ObserverRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range records {
		id := canonObserverID(records[i].ObserverID)
		if _, exists := r.observers[id]; exists {
			r.dirty[id] = struct{}{}
		}
	}
}

// Restore seeds the registry from persisted state at startup, before any
// collector runs.
func (r *ObserverRegistry) Restore(observers []ObserverRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range observers {
		o := observers[i]
		o.ObserverID = canonObserverID(o.ObserverID)
		if o.ObserverID == "" {
			continue
		}
		// Persisted rows predating id canonicalization may be upper-cased; merge any
		// duplicates that collapse to the same canonical key so counts stay correct.
		if existing := r.observers[o.ObserverID]; existing != nil {
			mergeObserver(existing, &o)
		} else {
			r.observers[o.ObserverID] = &o
		}
	}
}

// mergeObserver folds src into dst, keeping the widest activity window, summed
// observations, a non-empty name, and the union of networks.
func mergeObserver(dst, src *ObserverRecord) {
	if src.FirstSeen != 0 && (dst.FirstSeen == 0 || src.FirstSeen < dst.FirstSeen) {
		dst.FirstSeen = src.FirstSeen
	}
	if src.LastSeen > dst.LastSeen {
		dst.LastSeen = src.LastSeen
	}
	dst.Observations += src.Observations
	if dst.Name == "" {
		dst.Name = src.Name
	}
	for _, net := range src.Networks {
		if !containsStr(dst.Networks, net) {
			dst.Networks = append(dst.Networks, net)
		}
	}
}
