package main

import (
	"encoding/hex"
	"testing"

	"github.com/meshcore-cz/meshpkt"
)

func makeRawPacketHex(t *testing.T, typ meshpkt.PayloadType, hashSize int, hops int) string {
	t.Helper()
	rawPath := make([]byte, hashSize*hops)
	for i := range rawPath {
		rawPath[i] = byte(i + 1)
	}
	raw, err := meshpkt.EncodePacket(meshpkt.Packet{
		Route:        meshpkt.RouteFlood,
		Type:         typ,
		PathHashSize: hashSize,
		Path:         rawPath,
	})
	if err != nil {
		t.Fatalf("EncodePacket: %v", err)
	}
	return hex.EncodeToString(raw)
}

func newLinkCollector(reg *LinkRegistry) *Collector {
	return &Collector{
		net:   &NetworkState{ID: "test-net", Counter: newCounter()},
		az:    &AnalyzerState{Name: "az1", Counter: newCounter()},
		links: reg,
	}
}

func TestCollectorSkipsAmbiguousOneByteMultiHopLinks(t *testing.T) {
	a, b, c := pk(1), pk(2), pk(3)
	reg := noDecay()
	response := int(meshpkt.PayloadResponse)

	feedPacket(newLinkCollector(reg), wsPacket{
		Hash:         "h-low-response",
		RawHex:       makeRawPacketHex(t, meshpkt.PayloadResponse, 1, 3),
		PayloadType:  &response,
		ResolvedPath: []string{a, b, c},
	})

	if n := reg.linkCount(); n != 0 {
		t.Fatalf("link count = %d, want 0 for low-confidence non-TRACE path", n)
	}
}

func TestCollectorRecordsTwoByteAndTraceLinks(t *testing.T) {
	a, b, c := pk(1), pk(2), pk(3)
	response := int(meshpkt.PayloadResponse)
	trace := int(meshpkt.PayloadTrace)

	t.Run("two-byte response", func(t *testing.T) {
		reg := noDecay()
		feedPacket(newLinkCollector(reg), wsPacket{
			Hash:         "h-response",
			RawHex:       makeRawPacketHex(t, meshpkt.PayloadResponse, 2, 2),
			PayloadType:  &response,
			ResolvedPath: []string{a, b, c},
		})
		if n := reg.linkCount(); n != 2 {
			t.Fatalf("link count = %d, want 2", n)
		}
	})

	t.Run("one-byte trace", func(t *testing.T) {
		reg := noDecay()
		feedPacket(newLinkCollector(reg), wsPacket{
			Hash:         "h-trace",
			RawHex:       makeRawPacketHex(t, meshpkt.PayloadTrace, 1, 2),
			PayloadType:  &trace,
			ResolvedPath: []string{a, b, c},
		})
		if n := reg.linkCount(); n != 2 {
			t.Fatalf("link count = %d, want 2", n)
		}
	})
}
