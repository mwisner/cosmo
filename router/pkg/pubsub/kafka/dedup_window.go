package kafka

import (
	"encoding/binary"
	"hash/fnv"
	"os"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Duplicate-event deduplication for the Kafka subscription path.
//
// The live-shows topics re-publish the same message many times, and Cosmo processes and dispatches
// every received record individually (no native event dedup). This collapses near-simultaneous
// identical records at the earliest point — the poll loop, before Update() — so the whole
// downstream pipeline (BeforeEventsDispatch, subscription filtering, per-subscriber fan-out and
// _entities resolution) is skipped for the dropped copies.
//
// The guardrail is a very small time window: same-instant bursts collapse, while identical
// payloads re-emitted seconds apart (which for a contentless entity reference mean "re-resolve
// again") fall outside the window and are delivered untouched.

// dedupKeyMode selects which parts of a record form its identity for deduplication.
type dedupKeyMode int

const (
	// dedupKeyContent keys on (partition, key, value) — the default. Collapses byte-identical
	// payloads for the same Kafka key that arrive within the window.
	dedupKeyContent dedupKeyMode = iota
	// dedupKeyValue keys on the value only.
	dedupKeyValue
	// dedupKeyExact keys on (partition, key, value, timestamp). Only collapses records that are
	// byte-identical AND share the same producer timestamp, regardless of window — the strictest,
	// safest mode.
	dedupKeyExact
)

func parseDedupKeyMode(s string) dedupKeyMode {
	switch s {
	case "value":
		return dedupKeyValue
	case "exact":
		return dedupKeyExact
	default:
		return dedupKeyContent
	}
}

// dedupConfig holds the tunable levers for Kafka event deduplication. All are sourced from the
// environment (see dedupConfigFromEnv) so they can be toggled per-deployment without a rebuild.
type dedupConfig struct {
	enabled  bool
	windowMs int64 // suppression window in milliseconds; 0 = collapse only same-timestamp records
	keyMode  dedupKeyMode
	maxKeys  int // per-poller cap on tracked identities (bounds memory)
}

// dedupConfigFromEnv reads the KAFKA_DEDUP_* levers. Defaults: disabled, 50ms window, content key,
// 4096 keys. Invalid values fall back to the default for that lever.
func dedupConfigFromEnv() dedupConfig {
	cfg := dedupConfig{
		enabled:  false,
		windowMs: 50,
		keyMode:  dedupKeyContent,
		maxKeys:  4096,
	}
	if v, ok := os.LookupEnv("KAFKA_DEDUP_ENABLED"); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.enabled = b
		}
	}
	if v, ok := os.LookupEnv("KAFKA_DEDUP_WINDOW_MS"); ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			cfg.windowMs = n
		}
	}
	if v, ok := os.LookupEnv("KAFKA_DEDUP_KEY"); ok {
		cfg.keyMode = parseDedupKeyMode(v)
	}
	if v, ok := os.LookupEnv("KAFKA_DEDUP_MAX_KEYS"); ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.maxKeys = n
		}
	}
	return cfg
}

// dedupWindow tracks recently-delivered record identities to drop duplicates. It is NOT safe for
// concurrent use: each topicPoller (one goroutine per subscription) owns its own window, so no
// locking is needed and one subscription can never affect another's dedup state.
type dedupWindow struct {
	cfg      dedupConfig
	lastSeen map[uint64]int64 // identity hash -> last delivered record timestamp (unix millis)
}

// newDedupWindow returns a window for the given config, or nil when dedup is disabled. A nil
// window is safe to use — isDuplicate always returns false.
func newDedupWindow(cfg dedupConfig) *dedupWindow {
	if !cfg.enabled {
		return nil
	}
	return &dedupWindow{
		cfg:      cfg,
		lastSeen: make(map[uint64]int64),
	}
}

// isDuplicate reports whether r duplicates a record delivered within the window and should be
// dropped. When it returns false it records r as delivered. A nil window never deduplicates.
func (w *dedupWindow) isDuplicate(r *kgo.Record) bool {
	if w == nil {
		return false
	}
	h := w.identity(r)
	tsMs := r.Timestamp.UnixMilli()
	if last, ok := w.lastSeen[h]; ok && tsMs >= last && tsMs-last <= w.cfg.windowMs {
		return true
	}
	if len(w.lastSeen) >= w.cfg.maxKeys {
		w.evict(tsMs)
	}
	w.lastSeen[h] = tsMs
	return false
}

// identity hashes the parts of r that define a duplicate under the configured key mode. Fields are
// length-prefixed so "ab"+"c" and "a"+"bc" never collide.
func (w *dedupWindow) identity(r *kgo.Record) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	writeField := func(b []byte) {
		binary.LittleEndian.PutUint64(buf[:], uint64(len(b)))
		_, _ = h.Write(buf[:])
		_, _ = h.Write(b)
	}

	if w.cfg.keyMode != dedupKeyValue {
		// Partition guards against identical payloads on different partitions collapsing together.
		binary.LittleEndian.PutUint32(buf[:4], uint32(r.Partition))
		_, _ = h.Write(buf[:4])
		writeField(r.Key)
	}
	writeField(r.Value)
	if w.cfg.keyMode == dedupKeyExact {
		binary.LittleEndian.PutUint64(buf[:], uint64(r.Timestamp.UnixNano()))
		_, _ = h.Write(buf[:])
	}
	return h.Sum64()
}

// evict bounds memory when the window hits its cap: it drops entries older than the window first,
// and clears the window entirely if that is not enough. Clearing can at worst miss a future dedup;
// it can never cause a wrong drop.
func (w *dedupWindow) evict(nowMs int64) {
	for h, ts := range w.lastSeen {
		if nowMs-ts > w.cfg.windowMs {
			delete(w.lastSeen, h)
		}
	}
	if len(w.lastSeen) >= w.cfg.maxKeys {
		clear(w.lastSeen)
	}
}
