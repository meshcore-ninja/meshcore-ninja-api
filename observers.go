package main

import (
	"sort"
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

// Observe records one observer report: upserts the row, advances last-seen and
// the report count, and adds the network to the observer's set.
func (r *ObserverRegistry) Observe(a ObserverActivity) {
	if a.ObserverID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	o := r.observers[a.ObserverID]
	if o == nil {
		o = &ObserverRecord{ObserverID: a.ObserverID, FirstSeen: a.At}
		r.observers[a.ObserverID] = o
	}
	if a.Name != "" {
		o.Name = a.Name
	}
	o.LastSeen = a.At
	o.Observations++
	if a.NetworkID != "" && !containsStr(o.Networks, a.NetworkID) {
		o.Networks = append(o.Networks, a.NetworkID)
	}
	r.dirty[a.ObserverID] = struct{}{}
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
		if _, exists := r.observers[records[i].ObserverID]; exists {
			r.dirty[records[i].ObserverID] = struct{}{}
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
		r.observers[o.ObserverID] = &o
	}
}
