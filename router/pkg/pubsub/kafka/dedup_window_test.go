package kafka

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

const showRef = `{"__typename":"Show","id":"show_01kz6fvagtef5abnxbcb9036jj"}`

func rec(key, val string, partition int32, tsMs int64) *kgo.Record {
	return &kgo.Record{
		Key:       []byte(key),
		Value:     []byte(val),
		Partition: partition,
		Offset:    0,
		Timestamp: time.UnixMilli(tsMs),
		Topic:     "live.graphql.show-events",
	}
}

func newWindow(t *testing.T, windowMs int64, mode dedupKeyMode, maxKeys int) *dedupWindow {
	t.Helper()
	return newDedupWindow(dedupConfig{enabled: true, windowMs: windowMs, keyMode: mode, maxKeys: maxKeys})
}

// countDropped runs records through the window and returns how many were dropped as duplicates.
func countDropped(w *dedupWindow, records []*kgo.Record) int {
	dropped := 0
	for _, r := range records {
		if w.isDuplicate(r) {
			dropped++
		}
	}
	return dropped
}

func TestDedup_SameTimestampBurstCollapses(t *testing.T) {
	// The audit log's worst case: a run of 70 byte-identical Show refs at the same timestamp.
	// First is delivered; the other 69 collapse.
	w := newWindow(t, 50, dedupKeyContent, 4096)
	records := make([]*kgo.Record, 70)
	for i := range records {
		records[i] = rec("show_x", showRef, 31, 1000)
	}
	require.Equal(t, 69, countDropped(w, records))
}

func TestDedup_ReEmitsSecondsApartSurvive(t *testing.T) {
	// Identical contentless Show refs re-emitted ~2s apart are separate "re-resolve" signals and
	// must all be delivered under a small window.
	w := newWindow(t, 50, dedupKeyContent, 4096)
	records := []*kgo.Record{
		rec("show_x", showRef, 31, 1000),
		rec("show_x", showRef, 31, 3000),
		rec("show_x", showRef, 31, 5000),
		rec("show_x", showRef, 31, 7000),
	}
	require.Equal(t, 0, countDropped(w, records))
}

func TestDedup_DistinctPayloadsNeverCollapse(t *testing.T) {
	w := newWindow(t, 50, dedupKeyContent, 4096)
	records := []*kgo.Record{
		rec("lot_1", `{"__typename":"LotItem","id":"lot_1"}`, 31, 1000),
		rec("lot_2", `{"__typename":"LotItem","id":"lot_2"}`, 31, 1000),
		rec("lot_3", `{"__typename":"LotItem","id":"lot_3"}`, 31, 1000),
	}
	require.Equal(t, 0, countDropped(w, records))
}

func TestDedup_WindowZeroOnlyCollapsesSameTimestamp(t *testing.T) {
	w := newWindow(t, 0, dedupKeyContent, 4096)
	require.False(t, w.isDuplicate(rec("show_x", showRef, 31, 1000)))
	require.True(t, w.isDuplicate(rec("show_x", showRef, 31, 1000)), "same ms should collapse")
	require.False(t, w.isDuplicate(rec("show_x", showRef, 31, 1001)), "1ms later must survive when window=0")
}

func TestDedup_ExactModeIgnoresWindowAcrossTimestamps(t *testing.T) {
	// exact keys include the timestamp, so only byte-identical same-timestamp records collapse,
	// regardless of a non-zero window.
	w := newWindow(t, 50, dedupKeyExact, 4096)
	require.False(t, w.isDuplicate(rec("show_x", showRef, 31, 1000)))
	require.True(t, w.isDuplicate(rec("show_x", showRef, 31, 1000)), "same ts collapses")
	require.False(t, w.isDuplicate(rec("show_x", showRef, 31, 1010)), "diff ts within window still survives in exact mode")
}

func TestDedup_ValueModeIgnoresKey(t *testing.T) {
	w := newWindow(t, 50, dedupKeyValue, 4096)
	require.False(t, w.isDuplicate(rec("key_a", showRef, 31, 1000)))
	require.True(t, w.isDuplicate(rec("key_b", showRef, 31, 1000)), "same value, different key collapses in value mode")
}

func TestDedup_DifferentPartitionsDoNotCollapse(t *testing.T) {
	w := newWindow(t, 50, dedupKeyContent, 4096)
	require.False(t, w.isDuplicate(rec("show_x", showRef, 31, 1000)))
	require.False(t, w.isDuplicate(rec("show_x", showRef, 7, 1000)), "same content on a different partition must survive")
}

func TestDedup_NilWindowNeverDeduplicates(t *testing.T) {
	require.Nil(t, newDedupWindow(dedupConfig{enabled: false}))
	var w *dedupWindow
	require.False(t, w.isDuplicate(rec("show_x", showRef, 31, 1000)))
}

func TestDedup_MaxKeysBoundsMemory(t *testing.T) {
	// Many distinct keys all at the same timestamp force the clear() fallback; the map must stay
	// within the cap.
	w := newWindow(t, 50, dedupKeyContent, 8)
	for i := 0; i < 1000; i++ {
		w.isDuplicate(rec(fmt.Sprintf("k%d", i), showRef, 0, 1000))
		require.LessOrEqual(t, len(w.lastSeen), 8)
	}
}

func TestDedup_MixedFetchDeliveryCount(t *testing.T) {
	// A representative fetch: a same-timestamp Show burst interleaved with distinct LotItems and a
	// later re-emit. Delivered = unique-at-instant + all re-emits + all lot items.
	w := newWindow(t, 50, dedupKeyContent, 4096)
	records := []*kgo.Record{
		rec("show_x", showRef, 31, 1000), // delivered (first)
		rec("show_x", showRef, 31, 1000), // dropped
		rec("show_x", showRef, 31, 1000), // dropped
		rec("lot_1", `{"__typename":"LotItem","id":"lot_1"}`, 31, 1000), // delivered
		rec("lot_2", `{"__typename":"LotItem","id":"lot_2"}`, 31, 1000), // delivered
		rec("show_x", showRef, 31, 3000),                                // delivered (re-emit, 2s later)
	}
	require.Equal(t, 2, countDropped(w, records))
}

func TestDedupConfigFromEnv_Defaults(t *testing.T) {
	// No env set → safe defaults.
	t.Setenv("KAFKA_DEDUP_ENABLED", "")
	cfg := dedupConfigFromEnv()
	require.False(t, cfg.enabled)
	require.Equal(t, int64(50), cfg.windowMs)
	require.Equal(t, dedupKeyContent, cfg.keyMode)
	require.Equal(t, 4096, cfg.maxKeys)
}

func TestDedupConfigFromEnv_Overrides(t *testing.T) {
	t.Setenv("KAFKA_DEDUP_ENABLED", "true")
	t.Setenv("KAFKA_DEDUP_WINDOW_MS", "0")
	t.Setenv("KAFKA_DEDUP_KEY", "exact")
	t.Setenv("KAFKA_DEDUP_MAX_KEYS", "1024")
	cfg := dedupConfigFromEnv()
	require.True(t, cfg.enabled)
	require.Equal(t, int64(0), cfg.windowMs)
	require.Equal(t, dedupKeyExact, cfg.keyMode)
	require.Equal(t, 1024, cfg.maxKeys)
}

func TestDedupConfigFromEnv_InvalidValuesFallBack(t *testing.T) {
	t.Setenv("KAFKA_DEDUP_ENABLED", "notabool")
	t.Setenv("KAFKA_DEDUP_WINDOW_MS", "-5")
	t.Setenv("KAFKA_DEDUP_KEY", "bogus")
	t.Setenv("KAFKA_DEDUP_MAX_KEYS", "0")
	cfg := dedupConfigFromEnv()
	require.False(t, cfg.enabled)
	require.Equal(t, int64(50), cfg.windowMs)
	require.Equal(t, dedupKeyContent, cfg.keyMode)
	require.Equal(t, 4096, cfg.maxKeys)
}
