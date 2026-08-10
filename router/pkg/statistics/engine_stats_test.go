package statistics

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestEngineStatsSubscriptionObservations(t *testing.T) {
	t.Parallel()
	stats := NewEngineStats(t.Context(), zap.NewNop(), false)
	stats.SubscriptionResolutionError(SubscriptionResolutionErrorTimeout)
	stats.SubscriptionResolutionError(SubscriptionResolutionErrorTimeout)
	observation := WebSocketFrameObservation{FrameType: "data", PayloadType: "data", Result: "failure", Reason: "client_disconnected"}
	stats.WebSocketFrame(observation)

	report := stats.GetReport()
	require.Equal(t, []SubscriptionResolutionErrorCount{{Reason: SubscriptionResolutionErrorTimeout, Count: 2}}, report.SubscriptionResolutionErrors)
	require.Equal(t, []WebSocketFrameCount{{Observation: observation, Count: 1}}, report.WebSocketFrames)
}
