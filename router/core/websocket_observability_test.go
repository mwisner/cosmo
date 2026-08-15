package core

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wundergraph/cosmo/router/internal/wsproto"
	"github.com/wundergraph/cosmo/router/pkg/statistics"
	"go.uber.org/zap"
)

type observabilityTestProtocol struct {
	dataErr     error
	errorErr    error
	completeErr error
}

func (p *observabilityTestProtocol) Subprotocol() string                    { return wsproto.GraphQLWSSubprotocol }
func (p *observabilityTestProtocol) Initialize() (json.RawMessage, error)   { return nil, nil }
func (p *observabilityTestProtocol) ReadMessage() (*wsproto.Message, error) { return nil, nil }
func (p *observabilityTestProtocol) Pong(*wsproto.Message) error            { return nil }
func (p *observabilityTestProtocol) WriteGraphQLData(string, json.RawMessage, json.RawMessage) error {
	return p.dataErr
}
func (p *observabilityTestProtocol) WriteGraphQLErrors(string, json.RawMessage, json.RawMessage) error {
	return p.errorErr
}
func (p *observabilityTestProtocol) Complete(string) error { return p.completeErr }

func TestWebsocketResponseWriterObservesFrameOutcomes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		payload     string
		protocolErr error
		wantResult  string
		wantPayload string
	}{
		{name: "data success", payload: `{"data":{"value":1}}`, wantResult: "success", wantPayload: "data"},
		{name: "data with errors success", payload: `{"data":{"value":1},"errors":[{"message":"partial"}]}`, wantResult: "success", wantPayload: "data_with_errors"},
		{name: "errors only success", payload: `{"errors":[{"message":"failed"}],"data":null}`, wantResult: "success", wantPayload: "errors_only"},
		{name: "write failure", payload: `{"data":{"value":1}}`, protocolErr: errors.New("write failed"), wantResult: "failure", wantPayload: "data"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stats := statistics.NewEngineStats(t.Context(), zap.NewNop(), false)
			writer := newWebsocketResponseWriter("1", &observabilityTestProtocol{dataErr: tt.protocolErr}, true, zap.NewNop(), stats, nil)
			_, err := writer.Write([]byte(tt.payload))
			require.NoError(t, err)
			flushErr := writer.Flush()
			if tt.protocolErr == nil {
				require.NoError(t, flushErr)
			} else {
				require.ErrorIs(t, flushErr, tt.protocolErr)
			}

			report := stats.GetReport()
			require.Len(t, report.WebSocketFrames, 1)
			require.Equal(t, statistics.WebSocketFrameObservation{
				FrameType: "data", PayloadType: tt.wantPayload, Result: tt.wantResult,
				Reason: map[bool]string{true: "protocol_error", false: "none"}[tt.protocolErr != nil],
			}, report.WebSocketFrames[0].Observation)
			require.Equal(t, uint64(1), report.WebSocketFrames[0].Count)
		})
	}
}

func TestWebsocketResponseWriterObservesTerminalErrorWriteFailure(t *testing.T) {
	t.Parallel()
	stats := statistics.NewEngineStats(context.Background(), zap.NewNop(), false)
	writer := newWebsocketResponseWriter("1", &observabilityTestProtocol{errorErr: errors.New("client gone")}, true, zap.NewNop(), stats, nil)

	writer.Error([]byte(`{"errors":[{"message":"timeout"}]}`))

	report := stats.GetReport()
	require.Len(t, report.WebSocketFrames, 1)
	require.Equal(t, statistics.WebSocketFrameObservation{
		FrameType: "terminal_error", PayloadType: "errors_only", Result: "failure", Reason: "protocol_error",
	}, report.WebSocketFrames[0].Observation)
}
