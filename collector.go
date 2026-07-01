package main

import (
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/meshcore-cz/meshpkt"
)

// wsEnvelope is the outer frame CoreScope broadcasts on its WebSocket. We only
// care about {"type":"packet", ...}; everything else is ignored.
type wsEnvelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// wsPacket is the subset of a CoreScope packet record we consume. resolved_path
// is the list of full node public keys the packet traversed (sender + relays).
// If observer_id is also a full node key, link collection treats it as the final
// receiving hop. raw_hex is the wire-format packet, decoded locally for ADVERTs
// to extract node identity (pubkey, name, type, location).
type wsPacket struct {
	Hash         string   `json:"hash"`
	RawHex       string   `json:"raw_hex"`
	ObserverID   string   `json:"observer_id"`
	ObserverName string   `json:"observer_name"`
	PayloadType  *int     `json:"payload_type"`
	ResolvedPath []string `json:"resolved_path"`
}

// browserUA is sent on the Tangleveil handshake; some edges sit behind a WAF
// that is unhappy with the default Go client UA.
const browserUA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) " +
	"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"

// Collector handles packets for one analyzer stream after Tangleveil has
// multiplexed and routed the source.
type Collector struct {
	net       *NetworkState
	az        *AnalyzerState
	nodes     *NodeRegistry     // global node/advert registry (nil disables advert collection)
	observers *ObserverRegistry // global observer activity registry (nil disables it)
	links     *LinkRegistry     // global observed-link registry (nil disables it)
	metrics   *Metrics          // Prometheus telemetry (nil disables it)
}

func (c *Collector) handle(data []byte) {
	var env wsEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		c.metrics.recordDecodeError("envelope_json")
		return
	}
	if env.Type != "packet" || len(env.Data) == 0 {
		return
	}
	var p wsPacket
	if err := json.Unmarshal(env.Data, &p); err != nil {
		c.metrics.recordDecodeError("packet_json")
		return
	}
	hash := strings.ToLower(strings.TrimSpace(p.Hash))
	if hash == "" {
		c.metrics.recordDecodeError("empty_hash")
		return
	}
	typeName := ""
	if p.PayloadType != nil {
		typeName = meshpkt.PayloadType(byte(*p.PayloadType)).String()
	}
	now := nowUnix()
	c.metrics.recordPacket(c.net.ID, typeName)
	c.metrics.setAnalyzerLastPacket(c.net.ID, c.az.Name, now)
	c.net.Observe(c.az, Event{
		Hash:         hash,
		ObserverID:   p.ObserverID,
		ObserverName: p.ObserverName,
		PayloadType:  typeName,
		Nodes:        p.ResolvedPath,
		At:           now,
	})

	if c.observers != nil {
		c.observers.Observe(ObserverActivity{
			ObserverID: p.ObserverID,
			Name:       p.ObserverName,
			NetworkID:  c.net.ID,
			At:         now,
		})
	}

	// Observed links: record the adjacent node pairs in the resolved path. The
	// registry deduplicates globally by (packet hash, link) across observers and
	// networks, so this is fed every packet (not just adverts). The raw header
	// carries the route type (flood paths are observed adjacency, direct paths are
	// declared routes) and, for TRACE packets, a per-hop SNR accumulator.
	if c.links != nil {
		obs := PathObservation{
			Hash:      hash,
			NetworkID: c.net.ID,
			Path:      pathWithObserverReceiver(p.ResolvedPath, p.ObserverID),
			Now:       now,
		}
		if raw := decodeRawHex(p.RawHex); len(raw) > 0 {
			obs.RouteType = routeTypeName(raw[0])
			if pkt, err := meshpkt.DecodePacket(raw); err == nil {
				obs.LowConfidence = pkt.PathHashSize == 1 && pkt.HopCount() > 0
				if pkt.Type == meshpkt.PayloadTrace {
					obs.SNRs = meshpkt.TraceSNRs(pkt.Path)
				}
			}
		}
		if c.nodes != nil {
			obs.PosOf = c.nodes.PositionOf
		}
		c.links.ObservePathCtx(obs)
	}

	// ADVERT packets carry node identity. Decode the wire bytes locally and feed
	// the node registry + rolling latest-adverts feed.
	if c.nodes != nil && p.PayloadType != nil && meshpkt.PayloadType(byte(*p.PayloadType)) == meshpkt.PayloadAdvert {
		c.collectAdvert(p, hash, now)
	}
}

// decodeRawHex decodes a packet's raw_hex to wire bytes, or returns nil when it
// is absent or malformed (best-effort — many packet kinds may omit it).
func decodeRawHex(rawHex string) []byte {
	rawHex = strings.ToLower(strings.TrimSpace(rawHex))
	if rawHex == "" {
		return nil
	}
	raw, err := hex.DecodeString(rawHex)
	if err != nil {
		return nil
	}
	return raw
}

func pathWithObserverReceiver(path []string, observerID string) []string {
	observerID = strings.ToLower(strings.TrimSpace(observerID))
	if _, ok := normalizePub(observerID); !ok {
		return path
	}
	if len(path) > 0 && strings.EqualFold(strings.TrimSpace(path[len(path)-1]), observerID) {
		return path
	}
	out := append([]string(nil), path...)
	return append(out, observerID)
}

// routeTypeName maps the 2-bit route type in a packet header to a short label,
// used to break down which packet kinds a link's adjacency came from.
func routeTypeName(header byte) string {
	switch meshpkt.RouteType(header & 0x03) {
	case meshpkt.RouteTransportFlood:
		return "transport_flood"
	case meshpkt.RouteFlood:
		return "flood"
	case meshpkt.RouteDirect:
		return "direct"
	case meshpkt.RouteTransportDirect:
		return "transport_direct"
	default:
		return ""
	}
}

// collectAdvert decodes an ADVERT's raw wire bytes and records the node. Bad or
// truncated packets are silently dropped — the analyzer stream is best-effort.
func (c *Collector) collectAdvert(p wsPacket, hash string, now int64) {
	rawHex := strings.ToLower(strings.TrimSpace(p.RawHex))
	raw, err := hex.DecodeString(rawHex)
	if err != nil || len(raw) == 0 {
		c.metrics.recordDecodeError("advert_hex")
		return
	}
	pkt, err := meshpkt.DecodePacket(raw)
	if err != nil || pkt.Type != meshpkt.PayloadAdvert {
		c.metrics.recordDecodeError("advert_packet")
		return
	}
	adv, err := meshpkt.DecodeAdvertPayload(pkt.Payload)
	if err != nil {
		c.metrics.recordDecodeError("advert_payload")
		return
	}
	c.nodes.Observe(AdvertObservation{
		Hash:         hash,
		RawHex:       rawHex,
		PubKey:       hex.EncodeToString(adv.PublicKey),
		Name:         adv.Name,
		NodeType:     adv.NodeType,
		HasGPS:       adv.HasGPS,
		Lat:          adv.Lat,
		Lon:          adv.Lon,
		AdvertTime:   adv.Timestamp.Unix(),
		At:           now,
		NetworkID:    c.net.ID,
		AnalyzerName: c.az.Name,
		ObserverID:   p.ObserverID,
		ObserverName: p.ObserverName,
	})
}
